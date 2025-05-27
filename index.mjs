import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  HeadObjectCommand
} from "@aws-sdk/client-s3";

const bucketName = process.env.BUCKET_NAME;

const s3 = new S3Client({ region: "us-east-1" }); // change region as needed

export const handler = async (event) => {
  try {
    console.log("Received event:", JSON.stringify(event));
    const params = event.queryStringParameters || {};
    const callVolume = params.callVolume ?? 0;
    const putVolume = params.putVolume ?? 0;
    const lstPrice = params.lstPrice ?? 0;
    const selectedTicker = params.selectedTicker ?? 'SPY';

    const idTemp = Date.now().toString(36) + Math.random().toString(36).substring(2);
    const newEntry = {
      id: idTemp,
      timestamp: getTodayInEST(false),
      callVolume,
      putVolume,
      selectedTicker,
      lstPrice
    };

    const responseObject = await appendToS3JsonArray(newEntry);
    return {
      statusCode: 200,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*",
      },
      body: JSON.stringify(responseObject),
    };
  } catch (err) {
    console.error('Error:', err);
    return {
      statusCode: 500,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*",
      },
      body: JSON.stringify({ error: "Internal Server Error", details: err.message }),
    };
  }
};

const getTodayInEST = (isFileName) => {
  const estDate = new Date().toLocaleString("en-US", {
    timeZone: "America/New_York"
  });
  const date = new Date(estDate);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  const seconds = String(date.getSeconds()).padStart(2, "0");
  return isFileName ? `${year}-${month}-${day}` : `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
};

const streamToString = async (stream) => {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
  });
};

const checkIfFileExists = async (key) => {
  try {
    await s3.send(new HeadObjectCommand({ Bucket: bucketName, Key: key }));
    return true;
  } catch (err) {
    if (err.name === "NotFound") return false;
    throw err;
  }
};

const createJsonFile = async (key, data) => {
  await s3.send(new PutObjectCommand({
    Bucket: bucketName,
    Key: key,
    Body: JSON.stringify(data, null, 2),
    ContentType: "application/json",
  }));
};

const appendToS3JsonArray = async (newObject) => {
  const FILE_KEY = `${getTodayInEST(true)}.json`;
  let dataArray = [];

  const exists = await checkIfFileExists(FILE_KEY);
  if (!exists) {
    await createJsonFile(FILE_KEY, []);
    console.log('File created successfully');
  }

  const getCommand = new GetObjectCommand({ Bucket: bucketName, Key: FILE_KEY });
  const response = await s3.send(getCommand);
  const bodyString = await streamToString(response.Body);
  dataArray = JSON.parse(bodyString);

  const newId = (dataArray.at(-1)?.id || 0) + 1;
  const timestamp = new Date().toISOString();

  const newEntry = { id: newId, timestamp, ...newObject };
  dataArray.push(newEntry);

  const putCommand = new PutObjectCommand({
    Bucket: bucketName,
    Key: FILE_KEY,
    Body: JSON.stringify(dataArray, null, 2),
    ContentType: "application/json",
  });

  await s3.send(putCommand);
  console.log(`✅ Appended and uploaded JSON file to S3: ${FILE_KEY}`);
  return newEntry;
};
