// staticText.js
window.staticText = {
  // General App Text
  appTitle: "Etsy Listing Generator & Shop Listings",
  configureButtons: "Configure Buttons",

  // File Upload & Listing Input
  listingDescriptionHeader: "Listing Description",
  questionPlaceholder: "Enter your question here",
  dropCSVPlaceholder: "Drop CSV files here",
  
  // Etsy Connection & Listings
  connectToEtsy: "Connect to Etsy",
  updatePhotos: "Update Photos",
  updateInfo: "Update Info",
  etsyShopListings: "Etsy Shop Listings",
  shopListingsPlaceholder: "Enter up to 250 comma-separated 10-digit listing IDs",
  
  // Search Key Phrases
  searchKeyPhrases: "Search Key Phrases",
  clearCSV: "Clear CSV Files",
  analyzePhotos: "Analyze Photos",
  analyzeRules: "Analyze Rules",
  
  // Listing Generation
  generate: "Generate",
  etsyListingTitle: "Etsy Listing TITLE",
  reGen: "Re-Gen",
  copy: "Copy", // Text for copy buttons or labels
  titleCountText: "Count: 0",
  etsyListingDescription: "Etsy Listing DESCRIPTION",
  descriptionRulesButton: "Description Rules",
  
  // Preview Grid
  previewEmpty: "Empty",
  
  // Modal Texts
  modal: {
    photoMetadata: {
      title: "Photo Metadata",
      close: "Close"
    },
    analyzeRules: {
      title: "Photo Analysis Rules",
      defaultText: `Default Photo Analysis Rules:
1. Use SEO-friendly descriptive long-tail key phrases without special characters, focus on describing the jewelry and charm pendant, also focus and tailor Key Phrases and description to appropriate client based on the type of jewelry.
2. Emphasize charm details, focus image description on the type of jewelry and pendant description (what it is, occasion, metal, size, style).
3. Focus solely on the jewelry details especially the charm description.
4. Ignore environment, model, and clothing.
5. Organize into a short paragraph.
6. Limit the description to 300 characters.
7. Do not mention earrings unless explicitly identified.
8. Do not mix necklace and earring descriptions.`
    },
    titleRules: {
      title: "Title Rules",
      defaultText: `Title Structure & Formatting:
- Create 5-7 SEO optimized long-tail key phrases (3-5 words each).
- The title must begin with the product type (e.g., "Angel Earrings", "Daisy Necklace").
- Ensure grammatical flow and no punctuation.
- If the product is a charm/pendant, the first word should describe that type.
- For necklaces, the second word must be "necklace"; for earrings, "Earring".

Word Usage & Frequency:
- No word may appear more than three times.
- "earrings" must be plural and follow a noun.
- "stud" may only appear in approved phrases and must precede "earrings".

Required Words:
- "Charm" must appear exactly once.

Use of "For":
- "for" must appear at least twice in a grammatically correct manner.

Title Length & Word Order:
- Titles must be between 125 and 140 characters.
- Adjectives must follow nouns.
- Avoid plurals for "stud" and "animal"; use plurals for "hoops" and "earrings" correctly.

SEO Optimization:
- The title’s word order should form at least eight natural SEO phrases.

Prohibited Words:
- Exclude: "accessories", "unique", "simple", "minimalist", "whimsical", "cute", "filled", "gold filled", "silver", "solid gold", "gold vermeil", "rosegold", "14k", "handmade", "quirky", "delicate", "accessory", "for", "dangle", "gold plated", "jewelry", "custom", "celestial", "design", "lightweight".`
    },
    descriptionRules: {
      title: "Description Rules",
      defaultText: `Rules for Description:
- Merge the key phrases with your output.
- Modify the description to match the jewelry type.
- Keep the output paragraph to 80 words.

Option 1:
"This stunning gold Playing Card Charm is the perfect gift for her, featuring an intricately designed Ace of Spades charm. Ideal for jewelry lovers, this pendant is perfect for personalized pieces."

Option 2:
"This playful Allosaurus charm is the perfect add-on for charm necklaces or huggie hoops. It adds a whimsical touch to any collection, making it a delightful gift."

Option 3:
"This charming gold Alligator Necklace is a delightful gift for animal lovers. Featuring a beautifully designed crocodile pendant, it adds a unique touch to any collection."`
    },
    keywordRules: {
      title: "Keyword Rules",
      defaultText: `Using Sales Report and Product List Files:
- Extrapolate the best tag phrases based on performance.
- Focus on key terms like "Necklace", "Earrings", "Studs", "Hoops".
- Emphasize sales success; higher performing listings have more valuable keywords.
- Generate exactly 13 tag phrases, each ≤ 20 characters.
- Include "Gift for Her".
- "Charm" must appear at least once in a multi-word phrase (max 4 times total).
- "Necklace" appears no more than 3 times, "Gift" no more than 3 times, "Lover" no more than 2 times.
- Exclude: "Gift for Him", "Statement Piece", "Unique", "Spiritual Gift", "Outdoor Style", "accessories", "simple", "minimalist", "whimsical", "cute", "filled", "gold filled", "silver", "solid gold", "gold vermeil", "rosegold", "14k", "handmade", "quirky", "delicate", "accessory", "for", "dangle", "gold plated", "jewelry", "custom", "celestial", "design", "lightweight", "Fine Chain Necklace", "Small", "Large", "Nice", "Long", "Dark", "Short", "and", "but", "light", "heavy", "Wanderlust", "Theme", "Minimal Chain Gift", "Tiny Pendant Chain", "Charm Look", "Charm Wear", "Chain Necklace", "Small Pendant Chain", "Everyday"`
    },
    buttonConfig: {
      title: "Configure Button Positions",
      placeholder: "Uploaded files will appear here",
      saveButton: "Save Positions"
    },
    photoCrop: {
      title: "Crop Photo",
      save: "Save",
      cancel: "Cancel"
    },
    fileContent: {
      title: "File Content",
      close: "Close"
    }
  },
  
  // Appended description text for listings
  appendedDescriptionText: `

----------------------------------

D E T A I L S 
Materials: 
 •  Sterling Silver
 •  14k Gold Filled
 •  14K Rose Gold Filled 
 •  14K Solid Gold

• We use the Highest Quality materials from the US and Italy.
• Your purchase will come packaged in a lovely Jewelry Box

-----------------------------------

P A C K A G I N G
• Your purchase will come beautifully packaged. If you are ordering for a gift and would like each piece to be packaged separately please let me know. 

• If this purchase is a gift, and you would like us to include a handwritten message, leave a note in the "gift message" box at checkout.

------------------------------------

E X P E D I T E D • S H I P P I N G
You will be able to choose faster shipping options in the drop down menu when you check out. Ship times do NOT include production times (1-3 business days). However, if you select expedited shipping, we will try to get your order done faster.

------------------------------------

E X P L O R E • O U R • S H O P
Don't forget to check out the rest of our shop! We specialize in making handmade custom jewelry for every occasion. We take pride in making sure each order is made exactly to the customers specifications. We love collaborating with our customers to create  special and unique pieces for themselves and their loved ones. Please don't hesitate to contact us with any questions you have.  

http://custombrites.etsy.com

-----------------------------------

C H E C K • U S • O U T
Make sure you follow us on all our social media platforms to get a behind the scenes look, special promo codes, and some daily inspiration :) 

Instagram: https://www.instagram.com/britesjewelry
Facebook: https://www.facebook.com/britesjewelry
Pinterest Page : https://www.pinterest.com/britesjewelry

------------------------------------

S H O P  • P O L I C I E S
Check out our shop polices page for more details about all our polices and please don't hesitate to contact us with any questions you have! Happy Shopping :)

https://www.etsy.com/shop/CustomBrites/policy"

https://www.etsy.com/your/shops/me/listing-editor/edit/1822973998
`
};