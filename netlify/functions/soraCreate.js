 'use strict';
 const fetch = require('node-fetch');

 // Simple proxy to OpenAI Videos API: POST /v1/videos
 // Body: { prompt, model="sora-2", size="720x1280", seconds=4 }
 // Returns: job object { id, status, ... }
 exports.handler = async (event) => {
   try {
     if (event.httpMethod !== 'POST') {
       return { statusCode: 405, body: 'Method Not Allowed' };
     }

     const apiKey = process.env.OPENAI_API_KEY;
     if (!apiKey) {
       return { statusCode: 500, body: JSON.stringify({ error: 'Missing OPENAI_API_KEY' }) };
     }

     const body = JSON.parse(event.body || '{}');
     const prompt  = (body.prompt || '').trim();
     const model   = body.model   || 'sora-2';
     const size    = body.size    || '720x1280'; // 720x1280 or 1280x720 (sora-2); sora-2-pro also supports 1024x1792, 1792x1024
     const seconds = Number(body.seconds ?? 4);  // 4, 8, or 12

     if (!prompt) {
       return { statusCode: 400, body: JSON.stringify({ error: 'Missing "prompt"' }) };
     }
     if (![4,8,12].includes(seconds)) {
       return { statusCode: 400, body: JSON.stringify({ error: 'seconds must be 4, 8, or 12' }) };
     }

     const payload = { model, prompt, size, seconds };
     // (Optional next step) input_reference upload can be added later per cookbook

     const resp = await fetch('https://api.openai.com/v1/videos', {
       method: 'POST',
       headers: {
         'Content-Type': 'application/json',
         'Authorization': `Bearer ${apiKey}`
       },
       body: JSON.stringify(payload)
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