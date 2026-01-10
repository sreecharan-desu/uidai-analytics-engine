import mongoose, { Schema, Model } from 'mongoose';

const rawDataSchema = new Schema(
  {
    resource_id: { type: String, required: true, index: true },
    ingestion_timestamp: { type: Date, required: true, default: Date.now },
    source: { type: String, required: true, default: 'data.gov.in' },
  },
  { strict: false, timestamps: false, versionKey: false, bufferCommands: false }
);

// Compound indexes for faster L2 Cache lookups
rawDataSchema.index({ resource_id: 1, record_hash: 1 }, { unique: true });
rawDataSchema.index({ resource_id: 1, State: 1, District: 1 });
rawDataSchema.index({ resource_id: 1, Pincode: 1 });

// Create models for each collection
export const EnrolmentModel = mongoose.model('aadhaar_enrolment_raw', rawDataSchema);
export const DemographicModel = mongoose.model('aadhaar_demographic_update_raw', rawDataSchema);
export const BiometricModel = mongoose.model('aadhaar_biometric_update_raw', rawDataSchema);

export const getModelByResourceId = (resourceId: string): Model<any> | null => {
  // Mapping logic could be here, or passed from service
  // But strictly speaking, the service should decide.
  return null;
};
