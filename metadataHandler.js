(function() {
  // Embeds alt text into the EXIF data of a JPEG image using piexifjs.
  function embedAltTextInDataURL(originalDataURL, altText) {
    let exifObj = piexif.load(originalDataURL);
    exifObj["0th"][piexif.ImageIFD.ImageDescription] = altText;
    let exifBytes = piexif.dump(exifObj);
    let newDataURL = piexif.insert(exifBytes, originalDataURL);
    return newDataURL;
  }

  // Utility function to convert a data URL to a Blob.
  function dataURLtoBlob(dataurl) {
    const arr = dataurl.split(',');
    const mime = arr[0].match(/:(.*?);/)[1];
    const bstr = atob(arr[1]);
    let n = bstr.length;
    const u8arr = new Uint8Array(n);
    while (n--) {
      u8arr[n] = bstr.charCodeAt(n);
    }
    return new Blob([u8arr], { type: mime });
  }

  // Analyzes the image at the given index, embeds metadata into its EXIF,
  // saves the metadata into the global photoMeta array, and immediately triggers a grid update.
  async function analyzeAndEmbedMetadata(imageIndex) {
    if (!window.previewImages || window.previewImages.length <= imageIndex) {
      console.error("No image available to analyze at index " + imageIndex);
      return;
    }
    const dataUrl = window.previewImages[imageIndex];
    const blob = dataURLtoBlob(dataUrl);
    const formData = new FormData();
    formData.append("imageFile", blob, "analyzed_photo.jpg");
    const rulesElement = document.getElementById("analyzeRulesTextarea");
    const rules = rulesElement ? rulesElement.value : "Analyze the image and extract SEO-friendly metadata.";
    formData.append("rules", rules);

    try {
      const response = await fetch("/.netlify/functions/analyzeImage", {
        method: "POST",
        body: formData
      });
      if (!response.ok) {
        const errorText = await response.text();
        console.error("Error analyzing photo: " + errorText);
        return;
      }
      const result = await response.json();
      const metadata = result.metadata || "";
      const updatedDataURL = embedAltTextInDataURL(dataUrl, metadata);
      window.previewImages[imageIndex] = updatedDataURL;
      window.photoMeta = window.photoMeta || [];
      window.photoMeta[imageIndex] = metadata;
      console.log(`Metadata embedded into photo #${imageIndex + 1} successfully!`);
      // Immediately update the grid so the checkmark appears for this photo.
      if (typeof window.updateStaticPreviewGrid === "function") {
        window.updateStaticPreviewGrid();
      }
    } catch (error) {
      console.error("Error in metadataHandler:", error);
    }
  }

  // Expose the functions to the global scope.
  window.metadataHandler = {
    embedAltTextInDataURL: embedAltTextInDataURL,
    analyzeAndEmbedMetadata: analyzeAndEmbedMetadata
  };
})();