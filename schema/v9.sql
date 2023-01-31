-- Migration to add utility function

-- Function: sqlq.check_job_status
--
-- SQLQ.CHECK_JOB_STATUS(JOB_ID, STATE) checks if the job is in
-- a specific state.
CREATE OR REPLACE FUNCTION sqlq.check_job_status(job_id UUID, state sqlq.job_states)
RETURNS BOOLEAN AS $$
BEGIN
    IF (SELECT COUNT(*) FROM sqlq.jobs WHERE id = job_id AND status = state) THEN 
       RETURN TRUE;
    ELSE
       RETURN FALSE;
    END IF;
END;
$$ LANGUAGE plpgsql VOLATILE;


-- Function: sqlq.cancelling_job
--
-- SQLQ.CANCELLING_JOB(JOB_ID) change the state of a running or pending job to cancelling.
CREATE OR REPLACE FUNCTION sqlq.cancelling_job(job_id UUID)
RETURNS sqlq.job_states as $$
  UPDATE sqlq.jobs 
        SET status = 'cancelling'
   WHERE id = job_id AND status = 'running' OR status ='pending'
        RETURNING status;
$$ LANGUAGE SQL;


