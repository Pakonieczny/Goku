// netlify/functions/corsProxy.js
const https = require('https');

exports.handler = async (event, context) => {
  const imageUrl = event.queryStringParameters.url;

  if (!imageUrl) {
    return { statusCode: 400, body: "Missing 'url' parameter" };
  }

  return new Promise((resolve, reject) => {
    https.get(imageUrl, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => {
        const buffer = Buffer.concat(chunks);
        resolve({
          statusCode: 200,
          headers: {
            "Access-Control-Allow-Origin": "*", // This is the magic key
            "Content-Type": res.headers['content-type'],
            "Cache-Control": "public, max-age=86400"
          },
          body: buffer.toString('base64'),
          isBase64Encoded: true
        });
      });
    }).on('error', (e) => {
      resolve({ statusCode: 500, body: e.message });
    });
  });
};