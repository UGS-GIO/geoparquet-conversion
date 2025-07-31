const express = require('express');
const { Storage } = require('@google-cloud/storage');
const gdal = require('gdal-async');
const yauzl = require('yauzl');
const path = require('path');
const fs = require('fs').promises;
const os = require('os');

const app = express();
const port = process.env.PORT || 8080;

// Configuration constants
const PROCESSING_TIMEOUT = 3000000; // 50 minutes in milliseconds
const MAX_FILE_SIZE = 500 * 1024 * 1024; // 500 MB limit

// Initialize clients
const storage = new Storage();
const outputBucketName = process.env.OUTPUT_BUCKET || 'stagedparquet';

// Validate bucket name
if (!outputBucketName) {
  throw new Error('OUTPUT_BUCKET environment variable is required');
}

// Middleware
app.use(express.json());

// Health check endpoint
app.get('/', (req, res) => {
  res.json({ 
    status: 'ok', 
    message: 'Geospatial to Parquet converter (Cloud Run) - Supports GDB, Shapefiles, CSV',
    supportedFormats: ['Geodatabase (.gdb)', 'Shapefile (.shp)', 'CSV (.csv)'],
    outputBucket: outputBucketName,
    maxFileSize: MAX_FILE_SIZE
  });
});

// Eventarc endpoint for Cloud Storage events
app.post('/convert', async (req, res) => {
  const startTime = Date.now();
  
  try {
    const event = req.body;
    console.log('🐛 Event received:', JSON.stringify(event, null, 2));
    
    let eventData;
    if (event.kind === 'storage#object') {
      eventData = event;
      console.log('📦 Processing direct Cloud Storage notification');
    } else if (event.data) {
      eventData = event.data;
      console.log('⚡ Processing CloudEvent format');
    } else {
      eventData = event;
      console.log('🔧 Processing manual/other format');
    }

    console.log('🐛 Event data processed:', JSON.stringify(eventData, null, 2));
    
    const bucketName = eventData.bucket;
    const fileName = eventData.name;
    const fileSize = parseInt(eventData.size) || 0;

    console.log(`🚀 Starting conversion: ${fileName || 'undefined'} (${fileSize || 'undefined'} bytes)`);

    if (!fileName || !fileName.toLowerCase().endsWith('.zip')) {
      console.log(`⏭️  File ${fileName} is not a zip file. Skipping.`);
      return res.status(200).json({ message: 'File is not a zip file, skipping', fileName });
    }

    if (fileSize > MAX_FILE_SIZE) {
      console.error(`❌ File too large: ${fileName} (${fileSize} bytes > ${MAX_FILE_SIZE} bytes)`);
      return res.status(400).json({ error: 'File too large', fileName, fileSize, maxSize: MAX_FILE_SIZE });
    }

    console.log(`📁 Processing file: ${fileName} from bucket: ${bucketName}`);

    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(() => reject(new Error('Processing timeout')), PROCESSING_TIMEOUT);
    });

    const processingPromise = processFile(bucketName, fileName, fileSize, startTime);
    const result = await Promise.race([processingPromise, timeoutPromise]);
    
    res.status(200).json(result);

  } catch (error) {
    const processingTime = Date.now() - startTime;
    console.error(`❌ Conversion failed:`, {
      error: error.message,
      processingTime,
      stack: error.stack
    });
    
    res.status(500).json({
      error: 'Conversion failed',
      message: error.message,
      processingTime
    });
  }
});

// Manual trigger endpoint for testing
app.post('/convert-manual', async (req, res) => {
  const { bucketName, fileName } = req.body;
  
  if (!bucketName || !fileName) {
    return res.status(400).json({ error: 'bucketName and fileName are required' });
  }

  try {
    const [metadata] = await storage.bucket(bucketName).file(fileName).getMetadata();
    const fileSize = parseInt(metadata.size);
    const result = await processFile(bucketName, fileName, fileSize, Date.now());
    res.status(200).json(result);
  } catch (error) {
    console.error(`❌ Manual conversion failed:`, error);
    res.status(500).json({
      error: 'Manual conversion failed',
      message: error.message
    });
  }
});

async function processFile(bucketName, fileName, fileSize, startTime) {
  const tempDir = os.tmpdir();
  const localZipPath = path.join(tempDir, `${Date.now()}_${path.basename(fileName)}`);
  const processedFiles = [];
  const tempFiles = [localZipPath];

  try {
    console.log(`⬇️  Downloading ${fileName}...`);
    await storage.bucket(bucketName).file(fileName).download({ destination: localZipPath });
    console.log(`✅ Download complete`);

    console.log(`🔍 Inspecting zip file contents...`);
    const sources = await findSourcesInZip(localZipPath);
    if (sources.length === 0) {
      throw new Error('No convertible data sources (GDB, Shapefile, CSV) found in zip file');
    }
    
    console.log(`📂 Found ${sources.length} potential source(s) to process.`);

    for (const source of sources) {
      const { sourcePath, type } = source;
      const gdalInputPath = `/vsizip/${localZipPath}/${sourcePath}`;
      console.log(`Processing source: ${gdalInputPath} (Type: ${type})`);

      if (type === 'GDB') {
        const featureClasses = await getFeatureClasses(gdalInputPath);
        console.log(`🗃️  Found ${featureClasses.length} feature classes in GDB: ${featureClasses.join(', ')}`);
        if (featureClasses.length === 0) continue;

        for (const featureClass of featureClasses) {
          const outputFileName = `${path.basename(fileName, '.zip')}_${featureClass}.parquet`;
          const result = await processDataSource(gdalInputPath, outputFileName, { featureClass });
          processedFiles.push(result);
        }
      } else { // Handle Shapefile and CSV
        const baseOutputName = path.basename(sourcePath, path.extname(sourcePath));
        const outputFileName = `${path.basename(fileName, '.zip')}_${baseOutputName}.parquet`;
        const result = await processDataSource(gdalInputPath, outputFileName);
        processedFiles.push(result);
      }
    }
    
    const processingTime = Date.now() - startTime;
    console.log(`✅ Conversion task completed in ${processingTime}ms`);
    
    const successfulFiles = processedFiles.filter(f => f.status === 'success');
    const totalOutputSize = successfulFiles.reduce((sum, f) => sum + (f.outputSize || 0), 0);
    
    return {
      success: true,
      inputFile: fileName,
      outputFiles: processedFiles,
      inputSize: fileSize,
      totalOutputSize,
      processingTime,
      bucket: outputBucketName,
      summary: {
        total: processedFiles.length,
        successful: successfulFiles.length,
        skipped: processedFiles.filter(f => f.status === 'skipped').length,
        failed: processedFiles.filter(f => f.status === 'failed').length
      }
    };

  } finally {
    console.log(`🧹 Cleaning up temporary files...`);
    for (const tempFile of tempFiles) {
      await fs.unlink(tempFile).catch(err => 
        console.error(`Failed to delete temp file ${tempFile}: ${err.message}`)
      );
    }
    console.log(`🏁 Cleanup complete`);
  }
}

async function processDataSource(gdalInputPath, outputFileName, options = {}) {
  const tempDir = os.tmpdir();
  const tempOutputPath = path.join(tempDir, `${Date.now()}_${outputFileName}`);
  
  try {
    const [exists] = await storage.bucket(outputBucketName).file(outputFileName).exists();
    if (exists) {
      console.log(`✅ Output file ${outputFileName} already exists. Skipping.`);
      return {
        source: options.featureClass || gdalInputPath,
        outputFile: outputFileName,
        status: 'skipped',
        reason: 'already exists'
      };
    }

    await convertToParquet(gdalInputPath, tempOutputPath, options.featureClass);
    
    const stats = await fs.stat(tempOutputPath);
    console.log(`💾 Parquet file created: ${stats.size} bytes`);

    console.log(`⬆️  Uploading ${outputFileName} to bucket ${outputBucketName}...`);
    await storage.bucket(outputBucketName).upload(tempOutputPath, { destination: outputFileName });
    
    return {
      source: options.featureClass || path.basename(gdalInputPath),
      outputFile: outputFileName,
      outputSize: stats.size,
      status: 'success'
    };
  } catch (error) {
    console.error(`❌ Failed to process data source ${gdalInputPath}: ${error.message}`);
    return {
      source: options.featureClass || gdalInputPath,
      outputFile: outputFileName,
      status: 'failed',
      error: error.message
    };
  } finally {
    await fs.unlink(tempOutputPath).catch(() => {}); // Clean up the specific output file
  }
}

async function convertToParquet(gdalInputPath, outputPath, layerName = null) {
    const { exec } = require('child_process');
    const util = require('util');
    const execAsync = util.promisify(exec);

    const layerArg = layerName ? `"${layerName}"` : '';
    const ogr2ogrCmd = `ogr2ogr -f Parquet "${outputPath}" "${gdalInputPath}" ${layerArg} --config OGR_PARQUET_ALLOW_ALL_DIMS YES -makevalid -skipfailures -lco COMPRESSION=SNAPPY -lco EDGES=PLANAR -lco GEOMETRY_ENCODING=WKB -lco GEOMETRY_NAME=geometry -lco ROW_GROUP_SIZE=65536`;
    
    console.log(`🔧 Running system ogr2ogr: ${ogr2ogrCmd}`);
    try {
        await execAsync(ogr2ogrCmd);
        console.log(`✅ System ogr2ogr successful for ${gdalInputPath}`);
    } catch (error) {
        console.error(`⚠️  System ogr2ogr failed for ${gdalInputPath}: ${error.message}`);
        throw error; // Re-throw the error to be caught by the calling function
    }
}

async function getFeatureClasses(gdalInputPath) {
  try {
    const dataset = await gdal.openAsync(gdalInputPath);
    const featureClasses = [];
    for (let i = 0; i < dataset.layers.count(); i++) {
      featureClasses.push(dataset.layers.get(i).name);
    }
    dataset.close();
    return featureClasses;
  } catch (error) {
    console.error(`Failed to get feature classes from ${gdalInputPath}: ${error.message}`);
    throw error;
  }
}

function findSourcesInZip(zipPath) {
  return new Promise((resolve, reject) => {
    const sources = [];
    const foundGDBs = new Set();

    yauzl.open(zipPath, { lazyEntries: true }, (err, zipfile) => {
      if (err) return reject(err);
      
      zipfile.readEntry();
      zipfile.on('entry', (entry) => {
        const originalPath = entry.fileName;
        
        if (originalPath.includes('__MACOSX') || originalPath.startsWith('.')) {
          zipfile.readEntry();
          return;
        }

        const lowerCasePath = originalPath.toLowerCase();
        
        if (lowerCasePath.includes('.gdb/')) {
          const gdbPath = originalPath.substring(0, lowerCasePath.indexOf('.gdb') + 4);
          if (!foundGDBs.has(gdbPath)) {
            console.log(`🗃️  Found GDB: ${gdbPath}`);
            sources.push({ sourcePath: gdbPath, type: 'GDB' });
            foundGDBs.add(gdbPath);
          }
        } else if (lowerCasePath.endsWith('.shp')) {
          console.log(`🗺️  Found Shapefile: ${originalPath}`);
          sources.push({ sourcePath: originalPath, type: 'Shapefile' });
        } else if (lowerCasePath.endsWith('.csv')) {
          console.log(`📄 Found CSV: ${originalPath}`);
          sources.push({ sourcePath: originalPath, type: 'CSV' });
        }
        
        zipfile.readEntry();
      });
      
      zipfile.on('end', () => {
        console.log(`🔍 Zip inspection complete. Found ${sources.length} sources.`);
        resolve(sources);
      });
      
      zipfile.on('error', reject);
    });
  });
}

app.listen(port, () => {
  console.log(`🚀 Cloud Run service running on port ${port}`);
  console.log(`📊 Configuration:`);
  console.log(`   - Output bucket: ${outputBucketName}`);
  console.log(`   - Max file size: ${MAX_FILE_SIZE / 1024 / 1024}MB`);
  console.log(`   - Processing timeout: ${PROCESSING_TIMEOUT / 1000}s`);
  console.log(`   - Supported formats: GDB (multi-feature), Shapefile, CSV`);
});