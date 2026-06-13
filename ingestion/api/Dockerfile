FROM node:20-alpine

WORKDIR /app

COPY ingestion/api/package.json ./
RUN npm install --production

COPY shared/ ./shared/
COPY ingestion/api/src/ ./src/

EXPOSE 3000

CMD ["node", "src/app.js"]
