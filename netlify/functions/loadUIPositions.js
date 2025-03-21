// netlify/functions/loadUIPositions.js
const fs = require("fs");
const path = require("path");

exports.handler = async function (event, context) {
  try {
    const filePath = path.join("/tmp", "uiPositions.json");
    if (!fs.existsSync(filePath)) {
      return {
        statusCode: 200,
        body: JSON.stringify({ positions: {} })
      };
    }
    const data = fs.readFileSync(filePath, "utf8");
    const positions = JSON.parse(data);
    return {
      statusCode: 200,
      body: JSON.stringify({ positions })
    };
  } catch (error) {
    console.error("Error loading positions:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};