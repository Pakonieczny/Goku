 'use strict';
 const fetch = require('node-fetch');

 // Proxy to OpenAI Videos API: GET /v1/videos/{id}
 // Returns the job object; when complete, it includes downloadable asset(s)
 exports.handler = async (event) => {
   try {
     if (event.httpMethod !== 'GET') {
       return { statusCode: 405, body: 'Method Not Allowed' };
     }

     const apiKey = process.env.OPENAI_API_KEY;
     if (!apiKey) {
       return { statusCode: 500, body: JSON.stringify({ error: 'Missing OPENAI_API_KEY' }) };
     }

     const id = event.queryStringParameters?.id;
     if (!id) {
       return { statusCode: 400, body: JSON.stringify({ error: 'Missing "id"' }) };
     }

     const resp = await fetch(`https://api.openai.com/v1/videos/${encodeURIComponent(id)}`, {
       method: 'GET',
       headers: { 'Authorization': `Bearer ${apiKey}` }
     });
     const data = await resp.json();
     if (!resp.ok) {
       return { statusCode: resp.status, body: JSON.stringify({ error: data }) };
     }
     return { statusCode: 200, body: JSON.stringify(data) };
   } catch (err) {
     return { statusCode: 500, body: JSON.stringify({ error: String(err.message || err) }) };
   }
 };