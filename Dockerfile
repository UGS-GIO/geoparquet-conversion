# Use official GDAL image with full drivers (includes GeoParquet support)
FROM ghcr.io/osgeo/gdal:ubuntu-full-3.8.0

# Install Node.js and additional dependencies for Parquet support
RUN apt-get update && apt-get install -y \
    curl \
    libarrow-dev \
    libparquet-dev \
    && curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install Node.js dependencies
RUN npm ci --only=production

# Copy application code
COPY . .

# Expose port for Cloud Run
EXPOSE 8080

# Start the application
CMD ["npm", "start"]