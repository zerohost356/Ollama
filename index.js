const express = require("express");
const { Client } = require("@gradio/client");
const crypto = require("crypto");

const app = express();
const PORT = process.env.PORT || 3000;

const HF_IMAGE_MODELS = "Phr00t/Qwen-Image-Edit-Rapid-AIO";
const SPACE = "Sneak-Moose/Pro-Realism-Edit-Studio";

const imageCache = {};

function generateId() {
  return crypto.randomBytes(8).toString("hex");
}

// GET /api/images?prompt=...&image=<image_url>[&image2=<url>][&steps=4][&width=512][&height=512][&guidance=1]
app.get("/api/images", async (req, res) => {
  const startTime = Date.now();
  const {
    prompt,
    image,
    image2,
    steps = 4,
    width = 512,
    height = 512,
    guidance = 1,
  } = req.query;

  if (!prompt) {
    return res.status(400).json({ error: "Missing required query parameter: prompt" });
  }
  if (!image) {
    return res.status(400).json({
      error: "Missing required query parameter: image",
      hint: "This model edits images. Provide a source image URL via ?image=<url>",
    });
  }

  try {
    const client = await Client.connect(SPACE, {
      hf_token: process.env.HF_API_KEY,
    });

    const image1Input = { url: image, path: image, meta: { _type: "gradio.FileData" } };
    const image2Input = image2
      ? { url: image2, path: image2, meta: { _type: "gradio.FileData" } }
      : { url: image, path: image, meta: { _type: "gradio.FileData" } };

    const result = await client.predict("/infer", {
      image_1: image1Input,
      image_2: image2Input,
      prompt,
      seed: 0,
      randomize_seed: true,
      true_guidance_scale: Number(guidance),
      num_inference_steps: Number(steps),
      height: Number(height),
      width: Number(width),
    });

    const gallery = result.data[0];
    if (!gallery || gallery.length === 0) {
      return res.status(500).json({ error: "No image returned from model" });
    }

    const imageUrl = gallery[0].image?.url;
    if (!imageUrl) {
      return res.status(500).json({ error: "Could not extract image URL from result" });
    }

    const imgResponse = await fetch(imageUrl);
    if (!imgResponse.ok) {
      return res.status(500).json({ error: `Failed to fetch image: ${imgResponse.statusText}` });
    }

    const imageData = Buffer.from(await imgResponse.arrayBuffer()).toString("base64");
    const imageId = generateId();
    imageCache[imageId] = imageData;

    const duration = `${((Date.now() - startTime) / 1000).toFixed(2)}s`;
    const host = `${req.protocol}://${req.get("host")}`;
    const servedUrl = `${host}/generated/${imageId}.png`;

    return res.json({
      message: "Image generated successfully",
      status: "success",
      image: servedUrl,
      imageId,
      prompt,
      duration,
    });
  } catch (err) {
    const duration = `${((Date.now() - startTime) / 1000).toFixed(2)}s`;
    console.error("Error:", err.message);
    return res.status(500).json({ error: err.message, duration });
  }
});

// GET /generated/<imageId>.png
app.get("/generated/:imageId.png", (req, res) => {
  const imageData = imageCache[req.params.imageId];

  if (!imageData) {
    return res.status(404).json({ error: "Image not found" });
  }

  const imageBytes = Buffer.from(imageData, "base64");
  res.set({
    "Content-Type": "image/png",
    "Content-Length": imageBytes.length,
  });
  res.send(imageBytes);
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Model: ${HF_IMAGE_MODELS} via Space: ${SPACE}`);
  console.log(`Endpoint: GET /api/images?prompt=<prompt>&image=<image_url>`);
});
