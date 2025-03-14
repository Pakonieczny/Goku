const { Readable } = require("stream");
const formidable = require("formidable");

// Helper: convert a Buffer into a Readable stream
function bufferToStream(buffer) {
  const stream = new Readable();
  stream.push(buffer);
  stream.push(null);
  return stream;
}

exports.handler = async function (event, context) {
  try {
    // Ensure we have a content-type header.
    const contentType = event.headers["content-type"] || event.headers["Content-Type"];
    if (!contentType) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing content-type header" }),
      };
    }
    
    // Convert event.body to a Buffer, decoding from base64 if needed.
    let bodyBuffer;
    if (event.isBase64Encoded) {
      bodyBuffer = Buffer.from(event.body, "base64");
    } else {
      bodyBuffer = Buffer.from(event.body, "utf8");
    }
    
    // Create a Readable stream from the buffer.
    const req = bufferToStream(bodyBuffer);
    // Attach a headers property to the stream so that formidable can read it.
    req.headers = { "content-type": contentType };

    console.log("Starting form parsing...");
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
        
        // At this point you can process the fields and files as needed.
        // For example, you might forward the file data (files.file.path, etc.) to Etsy.
        
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