-- Migration to alter the type of sqlq.job_states.
ALTER TYPE sqlq.job_states ADD VALUE IF NOT EXISTS 'cancelling';
ALTER TYPE sqlq.job_states ADD VALUE IF NOT EXISTS 'cancelled';