FROM node:20-slim

WORKDIR /app

COPY processing/threshold_detector/package.json .
RUN npm install --production

COPY processing/threshold_detector/src/ src/

CMD ["node", "src/detector.js"]