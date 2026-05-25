/* netlify/functions/geminiImageProxy.js
   Synchronous wrapper so the browser can receive JSON.
   (Background functions return immediately and may return an empty body.)
*/

const impl = require("./geminiImageProxy-background");

exports.handler = impl.handler;