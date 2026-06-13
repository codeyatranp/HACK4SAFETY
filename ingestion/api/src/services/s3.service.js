/**
 * S3 cold archive service.
 * Archives sensor data older than 90 days to S3 in Parquet format.
 * Optional — only used if AWS credentials are configured.
 */

let s3Client = null;

function getS3Client() {
  if (s3Client) return s3Client;

  if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
    console.warn("[S3] No AWS credentials configured, archive disabled");
    return null;
  }

  const { S3Client } = require("@aws-sdk/client-s3");
  s3Client = new S3Client({
    region: process.env.AWS_REGION || "ap-south-1",
  });

  return s3Client;
}

/**
 * Archive a batch of readings to S3.
 * Called periodically (e.g., daily cron) for data older than 90 days.
 */
async function archiveToS3(readings, dateStr) {
  const client = getS3Client();
  if (!client) return;

  const bucket = process.env.AWS_S3_BUCKET || "geoguard-archive";
  const key = `sensor-archive/${dateStr}/data.json`;

  try {
    const { PutObjectCommand } = require("@aws-sdk/client-s3");
    await client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: JSON.stringify(readings),
        ContentType: "application/json",
        StorageClass: "STANDARD_IA", // Infrequent access — cheaper
      })
    );

    console.log(`[S3] Archived ${readings.length} readings to s3://${bucket}/${key}`);
  } catch (err) {
    console.error("[S3 Archive Error]", err.message);
  }
}

module.exports = { archiveToS3 };
