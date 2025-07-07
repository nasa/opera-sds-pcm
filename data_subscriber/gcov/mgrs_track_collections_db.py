import sqlite3
import json

class MGRSTrackFrameDB:
    def __init__(self, path):

        if not path:
            raise ValueError("Path to database file must be provided")

        self.path = path
        self.conn = sqlite3.connect(path)
        self.table_name = "mgrs_track_frame_db"
    
    def frame_number_to_mgrs_set_ids(self, frame_number):
        """
        Returns the MGRS set IDs associated with the given frame number.
        
        Args:
            frame_number: The frame number to query
        
        Returns:
            A list of MGRS set IDs associated with the given frame number
        """
        cursor = self.conn.cursor()
        query = f"""
            SELECT mgrs_set_id
            FROM {self.table_name}
            WHERE (
                SELECT EXISTS (
                    SELECT 1
                    FROM json_each(frames)
                    WHERE value = ?
                )
            )
            """
        cursor.execute(query, (frame_number,))
        return [row[0] for row in cursor.fetchall()]
    
    def mgrs_set_id_to_frames(self, mgrs_set_id):
        """
        Returns the frame numbers associated with the given MGRS set ID.
        
        Args:
            mgrs_set_id: The MGRS set ID to query
        
        Returns:
            The frame numbers associated with the given MGRS set ID
        """
        cursor = self.conn.cursor()
        query = f"""
            SELECT frames
            FROM {self.table_name}
            WHERE mgrs_set_id = ?
            """
        cursor.execute(query, (mgrs_set_id,))
        frames = []
        for row in cursor.fetchall():
            frames.extend([int(frame) for frame in json.loads(row[0])])
        return set(frames)
    
    def frame_number_to_frame_set(self, frame_number):
        """
        Returns the frame numbers associated with the given frame number.
        
        Args:
            frame_number: The frame number to query
        
        Returns:
            The frame numbers associated with the given frame number
        """
        frame_set = set()
        mgrs_set_ids = self.frame_number_to_mgrs_set_ids(frame_number)
        for set_id in mgrs_set_ids:
            frame_set.update(self.mgrs_set_id_to_frames(set_id))

        return frame_set

    def frame_number_to_mgrs_sets_with_frames(self, frame_number):
        """
        Returns the MGRS set IDs and frame numbers associated with the given frame number.

        TODO: slow with nested iterations, could be faster SQL query 

        Args:
            frame_number: The frame number to query
        
        Returns:
            A dict of form {mgrs_set_id: set(frame numbers)} associated with the given frame number
        """
        frame_set = self.frame_number_to_frame_set(frame_number)
        mgrs_set_ids = self.frame_number_to_mgrs_set_ids(frame_number)
        return {mgrs_set_id: set(frame for frame in frame_set if frame in self.mgrs_set_id_to_frames(mgrs_set_id)) for mgrs_set_id in mgrs_set_ids}