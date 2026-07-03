const { Schema, model } = require("mongoose");

const pdfSummarySchema = new Schema(
  {
    file: { type: String, ref: "File", required: true, index: true },
    firstWords: { type: String, required: true },
    wordCount: { type: Number, required: true },
    createdAt: { type: Date, default: () => new Date() },
  },
  { versionKey: false }
);

const PdfSummary = model("PdfSummary", pdfSummarySchema);

module.exports = { PdfSummary };
