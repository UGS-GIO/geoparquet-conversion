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

// Helper function to sanitize filenames
function sanitizeFileName(fileName) {
  return fileName
    .replace(/\s+/g, '_')           // Replace spaces with underscores
    .replace(/[^a-zA-Z0-9_-]/g, '') // Remove special characters except underscore and dash
    .replace(/_+/g, '_')            // Replace multiple underscores with single
    .replace(/^_|_$/g, '')          // Remove leading/trailing underscores
    .toLowerCase();                 // Convert to lowercase for consistency
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

    if (fileName && fileName.startsWith('temp')) {
      console.log(`Ignoring temp file: ${fileName}`);
      return res.status(200).json({ message: 'Temp file ignored', fileName });
    }

    // CHECK IF FILE EXISTS - This stops the retry loop
    try {
      const [exists] = await storage.bucket(bucketName).file(fileName).exists();
      if (!exists) {
        console.log(`File ${fileName} does not exist in bucket ${bucketName}, acknowledging event to stop retries`);
        return res.status(200).json({ 
          message: 'File not found, event acknowledged to stop retries', 
          fileName,
          bucketName 
        });
      }
      console.log(`✅ File ${fileName} exists in bucket ${bucketName}`);
    } catch (checkError) {
      console.log(`Cannot verify file existence: ${checkError.message}, acknowledging anyway to stop retries`);
      return res.status(200).json({ 
        message: 'File check failed, event acknowledged to stop retries', 
        error: checkError.message,
        fileName,
        bucketName
      });
    }

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
    
    // Return 200 instead of 500 to stop retry loops
    res.status(200).json({
      error: 'Conversion failed but acknowledged to stop retries',
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
  const localZipPath = path.join(tempDir, `${Date.now()}_${fileName}`);
  
  // Declare all variables at function level to avoid temporal dead zone
  let gdalInputPath = null;
  let tempExtractDir = null;
  let outputFileName, metadataFileName, tempOutputPath, tempMetadataPath;

  try {
    // 1. Download the zip file
    console.log(`⬇️  Downloading ${fileName}...`);
    await storage.bucket(bucketName).file(fileName).download({ destination: localZipPath });
    console.log(`✅ Download complete`);

    // 2. Extract metadata.json from zip FIRST
    console.log(`📋 Looking for metadata.json...`);
    let metadata = null;
    try {
      metadata = await extractMetadataFromZip(localZipPath);
      console.log('📝 Found metadata.json:', Object.keys(metadata));
    } catch (error) {
      console.log('⚠️  No metadata.json found or failed to parse:', error.message);
    }

    // 3. Find source data in zip
    console.log(`🔍 Inspecting zip file contents...`);
    const sourceDataPathInZip = await findSourceInZip(localZipPath);
    if (!sourceDataPathInZip) {
      throw new Error('No convertible data source found in zip file');
    }

    console.log(`📂 Found source data: ${sourceDataPathInZip}`);
    
    // 4. Generate output filenames based on source layer WITH SANITIZATION
    const baseFileName = sanitizeFileName(path.basename(fileName, '.zip'));
    const layerName = sanitizeFileName(path.basename(sourceDataPathInZip, path.extname(sourceDataPathInZip)));
    
    outputFileName = `${baseFileName}_${layerName}.parquet`;
    metadataFileName = `${baseFileName}_${layerName}.metadata.json`;
    
    tempOutputPath = path.join(tempDir, `${Date.now()}_${outputFileName}`);
    tempMetadataPath = path.join(tempDir, `${Date.now()}_${metadataFileName}`);
    
    console.log(`📁 Output files will be: ${outputFileName} and ${metadataFileName}`);
    
    // 5. Extract shapefile if it's a shapefile  
    if (sourceDataPathInZip.toLowerCase().endsWith('.shp')) {
      console.log(`🗂️  Extracting shapefile from zip...`);
      tempExtractDir = path.join(tempDir, `extracted_${Date.now()}`);
      await fs.mkdir(tempExtractDir, { recursive: true });
      
      const extractedShpPath = await extractShapefileFromZip(localZipPath, sourceDataPathInZip, tempExtractDir);
      gdalInputPath = extractedShpPath;
      console.log(`📂 Using extracted shapefile: ${gdalInputPath}`);
    } else {
      // For GDB or CSV, use vsizip path
      gdalInputPath = `/vsizip/${localZipPath}/${sourceDataPathInZip}`;
      console.log(`📂 Using vsizip path: ${gdalInputPath}`);
    }
    
    // 6. Convert to parquet WITHOUT embedded metadata
    console.log(`🔄 Converting with system ogr2ogr...`);
    
    const { exec } = require('child_process');
    const util = require('util');
    const execAsync = util.promisify(exec);
    
    // Simple ogr2ogr command without metadata injection
    const ogr2ogrCmd = `ogr2ogr -f Parquet "${tempOutputPath}" "${gdalInputPath}" --config OGR_PARQUET_ALLOW_ALL_DIMS YES --config SHAPE_RESTORE_SHX YES -makevalid -skipfailures -lco COMPRESSION=SNAPPY -lco EDGES=PLANAR -lco GEOMETRY_ENCODING=WKB -lco GEOMETRY_NAME=geometry -lco ROW_GROUP_SIZE=65536`;
    
    console.log(`🔧 Running: ${ogr2ogrCmd}`);
    await execAsync(ogr2ogrCmd);
    
    console.log(`✅ System ogr2ogr successful - Parquet format created`);
    
    // 7. Write metadata to separate file if it exists
    if (metadata) {
      console.log(`📝 Writing metadata to separate file: ${metadataFileName}`);
      await fs.writeFile(tempMetadataPath, JSON.stringify(metadata, null, 2));
    }
    
    // 8. Get output file stats
    const stats = await fs.stat(tempOutputPath);
    console.log(`💾 Parquet file created successfully: ${stats.size} bytes`);

    // 9. Upload parquet file
    console.log(`⬆️  Uploading ${outputFileName} to bucket ${outputBucketName}...`);
    await storage.bucket(outputBucketName).upload(tempOutputPath, {
      destination: outputFileName,
    });
    
    // 10. Upload metadata file if it exists
    if (metadata) {
      console.log(`⬆️  Uploading ${metadataFileName} to bucket ${outputBucketName}...`);
      await storage.bucket(outputBucketName).upload(tempMetadataPath, {
        destination: metadataFileName,
      });
    }
    
    const processingTime = Date.now() - startTime;
    console.log(`✅ Conversion completed successfully in ${processingTime}ms`);

    return {
      success: true,
      inputFile: fileName,
      outputFile: outputFileName,
      metadataFile: metadata ? metadataFileName : null,
      inputSize: fileSize,
      outputSize: stats.size,
      format: 'Parquet',
      processingTime,
      bucket: outputBucketName,
      hasMetadata: !!metadata
    };

  } finally {
    // Cleanup
    console.log(`🧹 Cleaning up temporary files...`);
    await fs.unlink(localZipPath).catch(err => 
      console.error(`Failed to delete temp zip: ${err.message}`)
    );
    if (tempOutputPath) {
      await fs.unlink(tempOutputPath).catch(err => 
        console.error(`Failed to delete temp output: ${err.message}`)
      );
    }
    if (tempMetadataPath) {
      await fs.unlink(tempMetadataPath).catch(err => 
        console.error(`Failed to delete temp metadata: ${err.message}`)
      );
    }
    // Clean up extracted shapefile directory
    if (tempExtractDir) {
      await fs.rmdir(tempExtractDir, { recursive: true }).catch(err =>
        console.error(`Failed to delete temp extract dir: ${err.message}`)
      );
    }
    console.log(`🏁 Cleanup complete`);
  }
}

// Add this new function to extract metadata from zip
async function extractMetadataFromZip(zipPath) {
  return new Promise((resolve, reject) => {
    yauzl.open(zipPath, { lazyEntries: true }, (err, zipfile) => {
      if (err) return reject(err);
      
      zipfile.readEntry();
      zipfile.on('entry', (entry) => {
        if (entry.fileName.toLowerCase().includes('metadata.json')) {
          zipfile.openReadStream(entry, (err, readStream) => {
            if (err) return reject(err);
            
            let content = '';
            readStream.on('data', chunk => content += chunk);
            readStream.on('end', () => {
              try {
                resolve(JSON.parse(content));
              } catch (parseErr) {
                reject(parseErr);
              }
            });
            readStream.on('error', reject);
          });
        } else {
          zipfile.readEntry();
        }
      });
      
      zipfile.on('end', () => reject(new Error('metadata.json not found')));
    });
  });
}

/**
 * Extract shapefile and all its components from zip
 */
async function extractShapefileFromZip(zipPath, shapefilePath, extractDir) {
  const shapefileBaseName = path.basename(shapefilePath, path.extname(shapefilePath));
  
  return new Promise((resolve, reject) => {
    yauzl.open(zipPath, { lazyEntries: true }, (err, zipfile) => {
      if (err) return reject(err);
      
      const filesToExtract = [];
      
      zipfile.readEntry();
      zipfile.on('entry', (entry) => {
        const entryBaseName = path.basename(entry.fileName, path.extname(entry.fileName));
        
        // Check if this file belongs to the same shapefile (same base name)
        if (entryBaseName === shapefileBaseName) {
          filesToExtract.push(entry);
        }
        zipfile.readEntry();
      });
      
      zipfile.on('end', async () => {
        console.log(`📦 Found ${filesToExtract.length} shapefile components to extract`);
        
        try {
          // Extract each component
          for (const entry of filesToExtract) {
            await extractSingleFile(zipPath, entry.fileName, extractDir);
            console.log(`✅ Extracted: ${entry.fileName}`);
          }
          resolve(path.join(extractDir, path.basename(shapefilePath)));
        } catch (extractError) {
          reject(extractError);
        }
      });
      
      zipfile.on('error', reject);
    });
  });
}

/**
 * Extract a single file from zip
 */
function extractSingleFile(zipPath, fileName, extractDir) {
  return new Promise((resolve, reject) => {
    yauzl.open(zipPath, { lazyEntries: true }, (err, zipfile) => {
      if (err) return reject(err);
      
      zipfile.readEntry();
      zipfile.on('entry', (entry) => {
        if (entry.fileName === fileName) {
          zipfile.openReadStream(entry, (err, readStream) => {
            if (err) return reject(err);
            
            const outputPath = path.join(extractDir, path.basename(fileName));
            const writeStream = require('fs').createWriteStream(outputPath);
            
            readStream.pipe(writeStream);
            writeStream.on('finish', () => resolve(outputPath));
            writeStream.on('error', reject);
            readStream.on('error', reject);
          });
        } else {
          zipfile.readEntry();
        }
      });
      
      zipfile.on('end', () => reject(new Error(`File ${fileName} not found in zip`)));
      zipfile.on('error', reject);
    });
  });
}

/**
 * Inspects a zip file and finds the first GDB, SHP, or CSV.
 * @param {string} zipPath Path to the local zip file.
 * @returns {Promise<string|null>} The path of the data source within the zip, or null.
 */
function findSourceInZip(zipPath) {
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