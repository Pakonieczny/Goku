// netlify/functions/saveUIPositions.js
const fs = require("fs");
const path = require("path");

exports.handler = async function (event, context) {
  try {
    if (!event.body) {
      throw new Error("No request body provided");
    }
    const { positions } = JSON.parse(event.body);
    if (!positions) {
      throw new Error("No positions provided");
    }
    // For demonstration: save positions to a temporary file.
    // Note: The /tmp directory is ephemeral and resets between deployments.
    const filePath = path.join("/tmp", "uiPositions.json");
    fs.writeFileSync(filePath, JSON.stringify(positions));
    return {
      statusCode: 200,
      body: JSON.stringify({ message: "Positions saved successfully." })
    };
  } catch (error) {
    console.error("Error saving positions:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};