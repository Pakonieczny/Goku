const { PassThrough } = require("stream");
const formidable = require("formidable");

// Helper: convert a Buffer into a PassThrough stream
function bufferToStream(buffer) {
  const stream = new PassThrough();
  stream.end(buffer);
  return stream;
}

exports.handler = async function (event, context) {
  try {
    // Ensure a content-type header exists.
    const contentType = event.headers["content-type"] || event.headers["Content-Type"];
    if (!contentType) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing content-type header" }),
      };
    }
    
    // Convert event.body to a Buffer, decoding from base64 if necessary.
    let bodyBuffer;
    if (event.isBase64Encoded) {
      bodyBuffer = Buffer.from(event.body, "base64");
    } else {
      bodyBuffer = Buffer.from(event.body, "utf8");
    }
    
    // Create a PassThrough stream from the buffer.
    const req = bufferToStream(bodyBuffer);
    req.headers = { "content-type": contentType };
    
    console.log("Starting form parsing with content-type:", contentType);
    
    // Parse the form data using formidable.
    return new Promise((resolve, reject) => {
      const form = new formidable.IncomingForm();
      form.parse(req, (err, fields, files) => {
        if (err) {
          console.error("Error parsing form data:", err);
          return reject({
            statusCode: 500,
            body: JSON.stringify({ error: err.message }),
          });
        }
        console.log("Parsed fields:", fields);
        console.log("Parsed files:", files);
        
        // Here you can process the fields and files as needed.
        resolve({
          statusCode: 200,
          body: JSON.stringify({ fields, files }),
        });
      });
    });
  } catch (error) {
    console.error("Exception in imageUpload handler:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};