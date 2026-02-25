-- Migration: Add raw_role column to preserve original role strings
-- This allows future improvements to role parsing without losing data

-- Add raw_role column (nullable for existing data)
ALTER TABLE credits ADD COLUMN raw_role TEXT;

-- For existing data, raw_role will be NULL
-- For new data, raw_role will contain the original role string from API

-- Example:
-- raw_role = "Animation Director (ep 10)"
-- role = "animation_director" (parsed)
