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
    message: 'Shapefile to GeoParquet converter (Cloud Run) - Multi-Feature Class Support',
    outputBucket: outputBucketName,
    maxFileSize: MAX_FILE_SIZE
  });
});

// Eventarc endpoint for Cloud Storage events
app.post('/convert', async (req, res) => {
  const startTime = Date.now();
  
  try {
    // Handle different event formats
    const event = req.body;
    console.log('🐛 Event received:', JSON.stringify(event, null, 2));
    
    // Check if it's a Cloud Storage notification (direct) or CloudEvent format
    let eventData;
    if (event.kind === 'storage#object') {
      // Direct Cloud Storage notification format
      eventData = event;
      console.log('📦 Processing direct Cloud Storage notification');
    } else if (event.data) {
      // CloudEvent format
      eventData = event.data;
      console.log('⚡ Processing CloudEvent format');
    } else {
      // Manual format or other
      eventData = event;
      console.log('🔧 Processing manual/other format');
    }

    console.log('🐛 Event data processed:', JSON.stringify(eventData, null, 2));
    
    const bucketName = eventData.bucket;
    const fileName = eventData.name;
    const fileSize = parseInt(eventData.size) || 0;

    console.log(`🚀 Starting conversion: ${fileName || 'undefined'} (${fileSize || 'undefined'} bytes)`);
    console.log(`Event ID: ${event.id || 'undefined'}`);
    console.log(`Event Type: ${event.eventType || event.type || 'undefined'}`);
    console.log(`Bucket: ${bucketName || 'undefined'}`);

    // Basic validation
    if (!fileName || !fileName.toLowerCase().endsWith('.zip')) {
      console.log(`⏭️  File ${fileName} is not a zip file. Skipping.`);
      return res.status(200).json({ message: 'File is not a zip file, skipping', fileName });
    }

    // File size check
    if (fileSize > MAX_FILE_SIZE) {
      console.error(`❌ File too large: ${fileName} (${fileSize} bytes > ${MAX_FILE_SIZE} bytes)`);
      return res.status(400).json({ error: 'File too large', fileName, fileSize, maxSize: MAX_FILE_SIZE });
    }

    console.log(`📁 Processing file: ${fileName} from bucket: ${bucketName}`);

    // Set processing timeout
    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(() => reject(new Error('Processing timeout')), PROCESSING_TIMEOUT);
    });

    // Main processing
    const processingPromise = processFile(bucketName, fileName, fileSize, startTime);

    // Race between processing and timeout
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
    // Get file metadata
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
  const localZipPath = path.join(tempDir, `${Date.now()}_${fileName}`);
  const processedFiles = [];
  const tempFiles = [localZipPath]; // Track all temp files for cleanup

  try {
    // 1. Download the zip file to a temporary location
    console.log(`⬇️  Downloading ${fileName}...`);
    await storage.bucket(bucketName).file(fileName).download({ destination: localZipPath });
    console.log(`✅ Download complete`);

    // 2. Inspect the zip file to find the source data
    console.log(`🔍 Inspecting zip file contents...`);
    const sourceInfo = await findSourceInZip(localZipPath);
    if (!sourceInfo) {
      console.log(`❌ No convertible data source (GDB, Shapefile, CSV) found in ${fileName}`);
      throw new Error('No convertible data source found in zip file');
    }
    
    const { sourcePath: sourceDataPathInZip, type: sourceType } = sourceInfo;
    
    // Construct the full virtual path for GDAL
    const gdalInputPath = `/vsizip/${localZipPath}/${sourceDataPathInZip}`;
    console.log(`📂 Found ${sourceType}: ${gdalInputPath}`);
    
    // 3. Check available drivers (for fallback only)
    console.log(`🔍 Checking available GDAL drivers...`);
    const drivers = gdal.drivers.getNames();
    console.log(`📋 Available Node.js GDAL drivers: ${drivers.slice(0, 10).join(', ')}... (${drivers.length} total)`);
    
    const hasParquetDriverInNodeGDAL = drivers.includes('Parquet');
    console.log(`🎯 Parquet driver in Node.js GDAL: ${hasParquetDriverInNodeGDAL}`);
    
    // Always try Parquet first with system ogr2ogr, as it's likely to have more drivers
    // The Node.js GDAL driver check is only used for fallback scenarios
    console.log(`⚙️  Will attempt Parquet format first using system ogr2ogr...`);
    gdal.config.set('OGR_PARQUET_ALLOW_ALL_DIMS', 'YES');
    
    let outputFormat = 'Parquet';
    let outputExtension = '.parquet';
    let formatOptions = [
      '-f', 'Parquet',
      '-makevalid',
      '-skipfailures',
      '-lco', 'COMPRESSION=SNAPPY',
      '-lco', 'EDGES=PLANAR',
      '-lco', 'GEOMETRY_ENCODING=WKB',
      '-lco', 'GEOMETRY_NAME=geometry',
      '-lco', 'ROW_GROUP_SIZE=65536'
    ];

    // 4. Handle different source types
    if (sourceType === 'GDB') {
      // For GDBs, process each feature class separately
      const featureClasses = await getFeatureClasses(gdalInputPath);
      console.log(`🗃️  Found ${featureClasses.length} feature classes in GDB: ${featureClasses.join(', ')}`);
      
      if (featureClasses.length === 0) {
        throw new Error('No feature classes found in geodatabase');
      }

      // Check for existing files and skip if already processed
      const toProcess = [];
      for (const featureClass of featureClasses) {
        const outputFileName = `${path.basename(fileName, '.zip')}_${featureClass}${outputExtension}`;
        try {
          const [exists] = await storage.bucket(outputBucketName).file(outputFileName).exists();
          if (exists) {
            console.log(`✅ Output file ${outputFileName} already exists. Skipping.`);
            processedFiles.push({
              featureClass,
              outputFile: outputFileName,
              status: 'skipped',
              reason: 'already exists'
            });
          } else {
            toProcess.push({ featureClass, outputFileName });
          }
        } catch (err) {
          console.log(`ℹ️  Could not check if ${outputFileName} exists: ${err.message}`);
          toProcess.push({ featureClass, outputFileName });
        }
      }

      // Process each feature class
      for (const { featureClass, outputFileName } of toProcess) {
        console.log(`🔄 Processing feature class: ${featureClass}`);
        
        const tempOutputPath = path.join(tempDir, `${Date.now()}_${featureClass}_${outputFileName}`);
        tempFiles.push(tempOutputPath);
        
        try {
          await processFeatureClass(gdalInputPath, featureClass, tempOutputPath, outputFormat, formatOptions);
          
          // Verify output file was created
          const stats = await fs.stat(tempOutputPath);
          console.log(`💾 ${outputFormat} file created for ${featureClass}: ${stats.size} bytes`);

          // Upload the resulting file to the output bucket
          console.log(`⬆️  Uploading ${outputFileName} to bucket ${outputBucketName}...`);
          await storage.bucket(outputBucketName).upload(tempOutputPath, {
            destination: outputFileName,
          });
          
          processedFiles.push({
            featureClass,
            outputFile: outputFileName,
            outputSize: stats.size,
            status: 'success'
          });
          
          console.log(`✅ Feature class ${featureClass} converted successfully`);
          
        } catch (fcError) {
          console.error(`❌ Failed to process feature class ${featureClass}: ${fcError.message}`);
          processedFiles.push({
            featureClass,
            outputFile: outputFileName,
            status: 'failed',
            error: fcError.message
          });
        }
      }
      
    } else {
      // For single files (Shapefile, CSV), process as before
      const outputFileName = `${path.basename(fileName, '.zip')}${outputExtension}`;
      
      // Check if output file already exists
      try {
        const [exists] = await storage.bucket(outputBucketName).file(outputFileName).exists();
        if (exists) {
          console.log(`✅ Output file ${outputFileName} already exists. Skipping reprocessing.`);
          return {
            success: true,
            inputFile: fileName,
            outputFiles: [{
              outputFile: outputFileName,
              status: 'skipped',
              reason: 'already exists'
            }],
            inputSize: fileSize,
            format: processedFiles.length > 0 ? processedFiles[0].format : 'Unknown',
            processingTime: Date.now() - startTime,
            bucket: outputBucketName
          };
        }
      } catch (err) {
        console.log(`ℹ️  Could not check if output exists: ${err.message}`);
      }
      
      const tempOutputPath = path.join(tempDir, `${Date.now()}_${outputFileName}`);
      tempFiles.push(tempOutputPath);
      
      await processSingleFile(gdalInputPath, tempOutputPath, hasParquetDriverInNodeGDAL);
      
      // Verify output file was created and determine actual format
      const stats = await fs.stat(tempOutputPath);
      const actualFormat = tempOutputPath.endsWith('.parquet') ? 'Parquet' : 'GeoJSON';
      console.log(`💾 ${actualFormat} file created successfully: ${stats.size} bytes`);

      // Upload the resulting file to the output bucket
      console.log(`⬆️  Uploading ${outputFileName} to bucket ${outputBucketName}...`);
      await storage.bucket(outputBucketName).upload(tempOutputPath, {
        destination: outputFileName,
      });
      
      processedFiles.push({
        outputFile: outputFileName,
        outputSize: stats.size,
        format: actualFormat,
        status: 'success'
      });
    }
    
    const processingTime = Date.now() - startTime;
    console.log(`✅ Conversion completed successfully in ${processingTime}ms`);
    
    const successfulFiles = processedFiles.filter(f => f.status === 'success');
    const totalOutputSize = successfulFiles.reduce((sum, f) => sum + (f.outputSize || 0), 0);
    
    console.log(`📊 Final stats: Input: ${fileName} (${fileSize} bytes) -> Output: ${processedFiles.length} files (${totalOutputSize} bytes total)`);

    return {
      success: true,
      inputFile: fileName,
      outputFiles: processedFiles,
      inputSize: fileSize,
      totalOutputSize,
      format: outputFormat,
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
    // Clean up all temporary files
    console.log(`🧹 Cleaning up ${tempFiles.length} temporary files...`);
    for (const tempFile of tempFiles) {
      await fs.unlink(tempFile).catch(err => 
        console.error(`Failed to delete temp file ${tempFile}: ${err.message}`)
      );
    }
    console.log(`🏁 Cleanup complete`);
  }
}

/**
 * Get all feature classes from a geodatabase
 */
async function getFeatureClasses(gdalInputPath) {
  try {
    const dataset = await gdal.openAsync(gdalInputPath);
    const featureClasses = [];
    
    for (let i = 0; i < dataset.layers.count(); i++) {
      const layer = dataset.layers.get(i);
      featureClasses.push(layer.name);
    }
    
    dataset.close();
    return featureClasses;
  } catch (error) {
    console.error(`Failed to get feature classes: ${error.message}`);
    throw error;
  }
}

/**
 * Process a single feature class from a geodatabase
 */
async function processFeatureClass(gdalInputPath, featureClassName, outputPath, hasParquetDriverInNodeGDAL) {
  try {
    // Try system ogr2ogr first for Parquet (most likely to succeed)
    try {
      const { exec } = require('child_process');
      const util = require('util');
      const execAsync = util.promisify(exec);
      
      // First try Parquet with system ogr2ogr
      const parquetOutputPath = outputPath.replace(/\.[^.]+$/, '.parquet');
      const ogr2ogrCmd = `ogr2ogr -f Parquet "${parquetOutputPath}" "${gdalInputPath}" "${featureClassName}" --config OGR_PARQUET_ALLOW_ALL_DIMS YES -makevalid -skipfailures -lco COMPRESSION=SNAPPY -lco EDGES=PLANAR -lco GEOMETRY_ENCODING=WKB -lco GEOMETRY_NAME=geometry -lco ROW_GROUP_SIZE=65536`;
      
      console.log(`🔧 Trying system ogr2ogr (Parquet): ${ogr2ogrCmd}`);
      await execAsync(ogr2ogrCmd);
      
      // If successful, rename to expected output path
      if (parquetOutputPath !== outputPath) {
        await fs.rename(parquetOutputPath, outputPath.replace(/\.[^.]+$/, '.parquet'));
      }
      
      console.log(`✅ System ogr2ogr successful (Parquet) for feature class ${featureClassName}`);
      return;
      
    } catch (ogrError) {
      console.log(`⚠️  System ogr2ogr (Parquet) failed for ${featureClassName}: ${ogrError.message}`);
      
      // Try system ogr2ogr with GeoJSON as fallback
      try {
        const { exec } = require('child_process');
        const util = require('util');
        const execAsync = util.promisify(exec);
        
        const geojsonOutputPath = outputPath.replace(/\.[^.]+$/, '.geojson');
        const ogr2ogrCmd = `ogr2ogr -f GeoJSON "${geojsonOutputPath}" "${gdalInputPath}" "${featureClassName}" -makevalid -skipfailures`;
        
        console.log(`🔧 Trying system ogr2ogr (GeoJSON): ${ogr2ogrCmd}`);
        await execAsync(ogr2ogrCmd);
        
        // If successful, rename to expected output path
        if (geojsonOutputPath !== outputPath) {
          await fs.rename(geojsonOutputPath, outputPath.replace(/\.[^.]+$/, '.geojson'));
        }
        
        console.log(`✅ System ogr2ogr successful (GeoJSON) for feature class ${featureClassName}`);
        return;
        
      } catch (geoJsonError) {
        console.log(`⚠️  System ogr2ogr (GeoJSON) also failed for ${featureClassName}: ${geoJsonError.message}`);
        console.log(`🔄 Falling back to Node.js GDAL...`);
      }
    }
    
    // Fall back to Node.js GDAL approach
    const inputDataset = await gdal.openAsync(gdalInputPath);
    const layer = inputDataset.layers.get(featureClassName);
    
    if (!layer) {
      throw new Error(`Feature class ${featureClassName} not found`);
    }
    
    // Determine format based on Node.js GDAL availability
    const useParquet = hasParquetDriverInNodeGDAL;
    const finalOutputPath = outputPath.replace(/\.[^.]+$/, useParquet ? '.parquet' : '.geojson');
    const driverName = useParquet ? 'Parquet' : 'GeoJSON';
    
    console.log(`🔧 Using Node.js GDAL with ${driverName} driver for ${featureClassName}`);
    
    // Create a new dataset with just this layer
    const driver = gdal.drivers.get(driverName);
    const outputDataset = await driver.createAsync(finalOutputPath);
    
    // Copy the layer
    await outputDataset.layers.copyAsync(layer);
    
    outputDataset.close();
    inputDataset.close();
    
    // Rename to expected output path if needed
    if (finalOutputPath !== outputPath) {
      await fs.rename(finalOutputPath, outputPath.replace(/\.[^.]+$/, useParquet ? '.parquet' : '.geojson'));
    }
    
    console.log(`✅ Node.js GDAL successful (${driverName}) for feature class ${featureClassName}`);
    
  } catch (error) {
    console.error(`Failed to process feature class ${featureClassName}: ${error.message}`);
    throw error;
  }
}

/**
 * Process a single file (Shapefile, CSV, etc.)
 */
async function processSingleFile(gdalInputPath, outputPath, outputFormat, formatOptions) {
  try {
    // Try system ogr2ogr first
    try {
      const { exec } = require('child_process');
      const util = require('util');
      const execAsync = util.promisify(exec);
      
      let ogr2ogrCmd;
      if (outputFormat === 'Parquet') {
        ogr2ogrCmd = `ogr2ogr -f Parquet "${outputPath}" "${gdalInputPath}" --config OGR_PARQUET_ALLOW_ALL_DIMS YES -makevalid -skipfailures -lco COMPRESSION=SNAPPY -lco EDGES=PLANAR -lco GEOMETRY_ENCODING=WKB -lco GEOMETRY_NAME=geometry -lco ROW_GROUP_SIZE=65536`;
      } else {
        ogr2ogrCmd = `ogr2ogr -f GeoJSON "${outputPath}" "${gdalInputPath}" -makevalid -skipfailures`;
      }
      
      console.log(`🔧 Running: ${ogr2ogrCmd}`);
      await execAsync(ogr2ogrCmd);
      console.log(`✅ System ogr2ogr successful`);
      
    } catch (ogrError) {
      console.log(`⚠️  System ogr2ogr failed: ${ogrError.message}`);
      console.log(`🔄 Falling back to Node.js GDAL...`);
      
      // Fall back to Node.js GDAL approach
      const inputDataset = await gdal.openAsync(gdalInputPath);
      await gdal.vectorTranslateAsync(outputPath, inputDataset, formatOptions);
      inputDataset.close();
    }
    
  } catch (error) {
    console.error(`Failed to process single file: ${error.message}`);
    throw error;
  }
}

/**
 * Inspects a zip file and finds the first GDB, SHP, or CSV.
 * @param {string} zipPath Path to the local zip file.
 * @returns {Promise<{sourcePath: string, type: string}|null>} The path and type of the data source within the zip, or null.
 */
function findSourceInZip(zipPath) {
  return new Promise((resolve, reject) => {
    let gdbPath = null;
    let shpPath = null;
    let csvPath = null;

    yauzl.open(zipPath, { lazyEntries: true }, (err, zipfile) => {
      if (err) {
        console.error(`Failed to open zip file: ${err.message}`);
        return reject(err);
      }
      
      zipfile.readEntry();
      
      zipfile.on('entry', (entry) => {
        const entryPath = entry.fileName.toLowerCase();
        const originalPath = entry.fileName;
        
        // Better GDB detection - look for .gdb directory structure
        if (entryPath.includes('.gdb/') && !gdbPath) {
          gdbPath = originalPath.split('.gdb/')[0] + '.gdb';
          console.log(`🗃️  Found GDB: ${gdbPath}`);
        } else if (entryPath.endsWith('.shp') && !shpPath) {
          shpPath = originalPath;
          console.log(`🗺️  Found Shapefile: ${shpPath}`);
        } else if (entryPath.endsWith('.csv') && !csvPath) {
          csvPath = originalPath;
          console.log(`📄 Found CSV: ${csvPath}`);
        }
        
        zipfile.readEntry();
      });
      
      zipfile.on('end', () => {
        let result = null;
        
        if (gdbPath) {
          result = { sourcePath: gdbPath, type: 'GDB' };
        } else if (shpPath) {
          result = { sourcePath: shpPath, type: 'Shapefile' };
        } else if (csvPath) {
          result = { sourcePath: csvPath, type: 'CSV' };
        }
        
        console.log(`🔍 Zip inspection complete. Selected: ${result ? `${result.sourcePath} (${result.type})` : 'No compatible data found'}`);
        resolve(result);
      });
      
      zipfile.on('error', (err) => {
        console.error(`Error reading zip entries: ${err.message}`);
        reject(err);
      });
    });
  });
}

app.listen(port, () => {
  console.log(`🚀 Cloud Run service running on port ${port}`);
  console.log(`📊 Configuration:`);
  console.log(`   - Output bucket: ${outputBucketName}`);
  console.log(`   - Max file size: ${MAX_FILE_SIZE / 1024 / 1024}MB`);
  console.log(`   - Processing timeout: ${PROCESSING_TIMEOUT / 1000}s`);
  console.log(`   - Multi-feature class support: Enabled`);
  
  // Log GDAL info
  try {
    const drivers = gdal.drivers.getNames();
    console.log(`   - GDAL drivers available: ${drivers.length}`);
    console.log(`   - Parquet support: ${drivers.includes('Parquet') ? 'Yes' : 'No'}`);
  } catch (err) {
    console.log(`   - GDAL status: Error loading (${err.message})`);
  }
});