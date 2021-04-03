SET search_path to addcolumns, public;
DROP FUNCTION  IF EXISTS expire_table_delete_old_rows;

CREATE FUNCTION expire_table_delete_old_rows() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  DELETE FROM metadata WHERE timestamp < NOW() - INTERVAL '10 sec';
  RETURN NEW;
END;
$$;
DROP TRIGGER  IF EXISTS  expire_table_delete_old_rows_trigger ON metadata;
CREATE TRIGGER expire_table_delete_old_rows_trigger
    AFTER INSERT ON metadata
    EXECUTE PROCEDURE expire_table_delete_old_rows();