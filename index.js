// Enhanced geoparquet converter for multi-layer geodatabase processing
// This should replace the existing single-layer processing logic

async function processMultiLayerGeodatabase(gdbPath, metadata) {
  const { schemaValidation } = metadata;
  
  if (!schemaValidation || !schemaValidation.selectedLayers) {
    console.log('No layer selection found, falling back to single-layer processing');
    return await processSingleLayer(gdbPath, metadata);
  }

  console.log(`Processing ${schemaValidation.selectedLayers.length} selected layers from geodatabase`);
  
  const results = [];
  const bucket = storage.bucket(OUTPUT_BUCKET);

  for (const layerMapping of schemaValidation.selectedLayers) {
    if (!layerMapping.enabled) {
      console.log(`Skipping disabled layer: ${layerMapping.sourceLayer}`);
      continue;
    }

    try {
      console.log(`Processing layer: ${layerMapping.sourceLayer} -> ${layerMapping.targetTable}`);
      
      // Generate output filename for this layer
      const layerOutputName = `${layerMapping.targetTable}.parquet`;
      const tempDir = `/tmp/${uuidv4()}`;
      const tempParquetPath = path.join(tempDir, layerOutputName);
      
      await fs.ensureDir(tempDir);

      // Convert specific layer to parquet using ogr2ogr
      const ogr2ogrArgs = [
        '-f', 'Parquet',
        tempParquetPath,
        gdbPath,
        layerMapping.sourceLayer, // Specify the layer name
        '-lco', 'COMPRESSION=SNAPPY',
        '-lco', 'GEOMETRY_ENCODING=WKB',
        '--config', 'CPL_DEBUG', 'ON'
      ];

      console.log(`Running ogr2ogr for layer ${layerMapping.sourceLayer}:`, ogr2ogrArgs);
      
      const result = await execGdalCommand('ogr2ogr', ogr2ogrArgs);
      
      if (result.exitCode !== 0) {
        throw new Error(`ogr2ogr failed for layer ${layerMapping.sourceLayer}: ${result.stderr}`);
      }

      // Verify parquet file was created
      if (!await fs.pathExists(tempParquetPath)) {
        throw new Error(`Parquet file not created for layer ${layerMapping.sourceLayer}`);
      }

      // Upload parquet file to staging bucket
      const file = bucket.file(layerOutputName);
      await file.save(await fs.readFile(tempParquetPath));
      console.log(`Uploaded parquet: ${layerOutputName}`);

      // Create metadata file for this layer
      const layerMetadata = {
        ...metadata,
        // Override with layer-specific information
        table_name: layerMapping.targetTable,
        source_layer: layerMapping.sourceLayer,
        schemaValidation: {
          ...schemaValidation,
          currentLayer: layerMapping,
          // Keep full selectedLayers for context, but mark current one
        }
      };

      const metadataFileName = `${layerMapping.targetTable}.metadata.json`;
      const metadataFile = bucket.file(metadataFileName);
      await metadataFile.save(JSON.stringify(layerMetadata, null, 2));
      console.log(`Uploaded metadata: ${metadataFileName}`);

      results.push({
        layer: layerMapping.sourceLayer,
        targetTable: layerMapping.targetTable,
        parquetFile: layerOutputName,
        metadataFile: metadataFileName,
        status: 'success'
      });

      // Cleanup temp files
      await fs.remove(tempDir);

    } catch (error) {
      console.error(`Failed to process layer ${layerMapping.sourceLayer}:`, error);
      results.push({
        layer: layerMapping.sourceLayer,
        targetTable: layerMapping.targetTable,
        status: 'failed',
        error: error.message
      });
    }
  }

  console.log('Multi-layer processing complete:', results);
  return results;
}

// Enhanced file processing entry point
async function processFile(eventData) {
  const { name: fileName, bucket: bucketName } = eventData;
  
  try {
    // Download and extract the zip file
    const extractedPath = await downloadAndExtract(bucketName, fileName);
    
    // Load metadata
    const metadata = await loadMetadata(fileName);
    
    // Determine if this is a multi-layer geodatabase
    const isMultiLayer = metadata?.schemaValidation?.selectedLayers?.length > 1;
    
    if (isMultiLayer) {
      console.log('Multi-layer geodatabase detected');
      await processMultiLayerGeodatabase(extractedPath, metadata);
    } else {
      console.log('Single-layer processing');
      await processSingleLayer(extractedPath, metadata);
    }
    
  } catch (error) {
    console.error('File processing failed:', error);
    throw error;
  }
}

// Helper function to load metadata from zip or external source
async function loadMetadata(fileName) {
  try {
    // First try to load from the uploaded zip file
    const bucket = storage.bucket('stagedzips');
    const metadataFileName = fileName.replace('.zip', '.metadata.json');
    const metadataFile = bucket.file(metadataFileName);
    
    const [exists] = await metadataFile.exists();
    if (exists) {
      const [content] = await metadataFile.download();
      return JSON.parse(content.toString());
    }
    
    // Fall back to checking inside the zip file
    // Implementation depends on your zip structure
    console.warn('No external metadata found, checking inside zip...');
    return null;
    
  } catch (error) {
    console.error('Error loading metadata:', error);
    return null;
  }
}