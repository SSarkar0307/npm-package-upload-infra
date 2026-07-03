const { Schema, model } = require("mongoose");

const fileSchema = new Schema(
  {
    _id: { type: String, required: true },
    key: { type: String, required: true },
    bucket: { type: String, required: true },
    originalName: { type: String, required: true },
    mimeType: { type: String, required: true },
    size: { type: Number, required: true },
    etag: { type: String, required: true },
    uploadedAt: { type: Date, required: true },
  },
  { versionKey: false }
);

const File = model("File", fileSchema);

module.exports = { File };
