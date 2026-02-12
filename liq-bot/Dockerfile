FROM node:20-slim

WORKDIR /app

# Copy package files first for layer caching
COPY package.json ./
RUN npm install --production

# Copy source
COPY . .

# Expose dashboard port
EXPOSE 3000

# Run with max performance flags
CMD ["node", "--max-old-space-size=256", "--expose-gc", "src/index.js"]
