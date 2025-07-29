GeoParquet Conversion Service
A Cloud Run service that automatically converts geospatial data files (shapefiles, geodatabases, CSV) to GeoParquet format when uploaded to Google Cloud Storage.
Features

Automatic Processing: Triggers on file upload to Cloud Storage
Multiple Formats: Supports GDB, Shapefile, and CSV inputs
GeoParquet Output: Efficient columnar storage with Snappy compression
Error Handling: Skips problematic layers and continues processing
Duplicate Prevention: Avoids reprocessing existing files