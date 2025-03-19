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
  // updates the global previewImages and photoMeta arrays, and displays metadata in the modal.
  async function analyzeAndEmbedMetadata(imageIndex) {
    if (!window.previewImages || window.previewImages.length <= imageIndex) {
      M.toast({ html: "No image available to analyze." });
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
        M.toast({ html: "Error analyzing photo: " + errorText });
        return;
      }
      const result = await response.json();
      const metadata = result.metadata || "No metadata returned.";
      const updatedDataURL = embedAltTextInDataURL(dataUrl, metadata);
      window.previewImages[imageIndex] = updatedDataURL;
      window.photoMeta = window.photoMeta || [];
      window.photoMeta[imageIndex] = metadata;
      const metadataTextarea = document.getElementById("metadataTextarea");
      if (metadataTextarea) {
        metadataTextarea.value = metadata;
      }
      M.toast({ html: `Metadata embedded into photo #${imageIndex + 1} successfully!` });
    } catch (error) {
      console.error("Error in metadataHandler:", error);
      M.toast({ html: "Exception: " + error.message });
    }
  }

  // Expose the functions to the global scope.
  window.metadataHandler = {
    embedAltTextInDataURL: embedAltTextInDataURL,
    analyzeAndEmbedMetadata: analyzeAndEmbedMetadata
  };
})();