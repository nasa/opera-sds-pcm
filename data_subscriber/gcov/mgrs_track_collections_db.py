import sqlite3
import json
from functools import cache
import geopandas as gpd
from pyproj import Transformer
from shapely.io import to_geojson


class MGRSTrackFrameDB:
    def __init__(self, path):

        if not path:
            raise ValueError("Path to database file must be provided")

        self.path = path
        self.conn = sqlite3.connect(path)
        self.table_name = "mgrs_track_frame_db"
    
    def frame_number_to_mgrs_set_ids(self, frame_number: int) -> list[str]:
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

    @cache
    def mgrs_set_id_to_frames(self, mgrs_set_id: int) -> set[int]:
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

    @cache
    def mgrs_set_id_to_track_frames(self, mgrs_set_id: int) -> list[str]:
        """
        Returns the track_frame numbers associated with the given MGRS set ID.

        Args:
            mgrs_set_id: The MGRS set ID to query

        Returns:
            The track_frame numbers associated with the given MGRS set ID
        """
        cursor = self.conn.cursor()
        query = f"""
            SELECT track_frame
            FROM {self.table_name}
            WHERE mgrs_set_id = ?
            """
        cursor.execute(query, (mgrs_set_id,))
        frames = []
        for row in cursor.fetchall():
            frames.extend([frame for frame in json.loads(row[0].replace("'", '"'))])
        return frames

    @cache
    def get_lof_for_mgrs_set_id(self, mgrs_set_id: str) -> str:
        """
        Returns the land_ocean_flag associated with the given MGRS set ID.

        Args:
            mgrs_set_id: The MGRS set ID to query

        Returns:
            The land_ocean_flag associated with the given MGRS set ID
        """

        cursor = self.conn.cursor()
        query = f"""
                    SELECT land_ocean_flag
                    FROM {self.table_name}
                    WHERE mgrs_set_id = ?
                    """
        cursor.execute(query, (mgrs_set_id,))

        flags = []

        for row in cursor.fetchall():
            flags.append(row[0])

        flags = list(set(flags))

        if len(flags) == 0:
            raise ValueError(f'MGRS set ID {mgrs_set_id} could not be associated with a land_ocean_flag')
        elif len(flags) > 1:
            raise ValueError(f'MGRS set ID {mgrs_set_id} mapped to more than one land_ocean_flag. {flags} '
                             f'This should not happen. ')

        return flags[0]

    def frame_number_to_frame_set(self, frame_number: int) -> set[int]:
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

    def frame_number_to_mgrs_sets_with_frames(self, frame_number: int) -> dict[str, set[int]]:
        """
        Returns the MGRS set IDs and frame numbers associated with the given frame number.
        
        Uses a single SQL query to get all MGRS sets containing the specified frame
        and all frames in those sets.

        Args:
            frame_number: The frame number to query
        
        Returns:
            A dict of form {mgrs_set_id: set(frame numbers)} associated with the given frame number
        """
        cursor = self.conn.cursor()
        query = f"""
            SELECT mgrs_set_id, frames
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
        
        result = {}
        for row in cursor.fetchall():
            mgrs_set_id = row[0]
            frames = set([int(frame) for frame in json.loads(row[1])])
            result[mgrs_set_id] = frames
            
        return result
        
    def frame_numbers_to_mgrs_sets_with_frames(self, frame_numbers: list[int]) -> dict[str, set[int]]:
        """
        Returns the MGRS set IDs and frame numbers associated with any of the given frame numbers.
        
        Args:
            frame_numbers: A list of frame numbers to query
        
        Returns:
            A dict of form {mgrs_set_id: set(frame numbers)} associated with the given frame numbers
        """
        if not frame_numbers:
            return {}
        
        # Remove duplicates from frame numbers
        unique_frames = list(set(frame_numbers))

        # query all mgrs sets containing any of the unique frames
        query = f"""
            SELECT DISTINCT mgrs_set_id, frames
            FROM {self.table_name}
            WHERE (
                SELECT EXISTS (
                    SELECT 1
                    FROM json_each(frames)
                    WHERE value IN ({','.join(map(str, unique_frames))})
                )
            )
        """
        
        # Execute query and process results
        cursor = self.conn.cursor()
        cursor.execute(query)
        result = {}
        for row in cursor.fetchall():
            mgrs_set_id = row[0]
            frames = set([int(f) for f in json.loads(row[1])])
            result[mgrs_set_id] = frames
        
        return result

    def frame_and_track_to_mgrs_sets(self, frame_track_tuples: set[tuple[int, int]]) -> dict[str, set[tuple[int, int]]]:
        """
        Returns a dict mapping mgrs_set_id (key) to track-frames tuples found in the DB.

        Args:
            frame_track_tuples: Set of (frame, track_id) tuples to query

        Returns:
            Dict of form {mgrs_set_id: {'track_number': ..., 'frames': set(...)}}
        """
        if not frame_track_tuples:
            return []

        # Build WHERE clause for (frame, track) pairs
        conditions = []
        params = []
        for frame, track in frame_track_tuples:
            conditions.append(f"(EXISTS (SELECT 1 FROM json_each(track_frame) WHERE value = ?))")
            params.append(f'{track}_{frame}')
        where_clause = " OR ".join(conditions)

        query = f"""
            SELECT mgrs_set_id, track_frame
            FROM {self.table_name}
            WHERE {where_clause}
        """

        cursor = self.conn.cursor()
        cursor.execute(query, params)
        results = {}
        for row in cursor.fetchall():
            mgrs_set_id = row[0]
            track_frames = [(int(f.split('_')[0]), int(f.split('_')[1])) for f in json.loads(row[1].replace("'", '"'))]
            results[mgrs_set_id] = set(track_frames)
        return results

    def track_and_frame_to_all_frames(self, track_number: int, frame_number: int) -> set[tuple[int, int]]:
        """
        For a given track number and frame number, returns the set of all tracks & frames in all frame sets with that
        track and frame.

        Args:
            track_number: The track number to query
            frame_number: The frame number to query

        Returns:
            Set of track_frame number tuples associated with the given track number and frame number
        """

        query = f"""
            SELECT track_frame
            FROM {self.table_name}
            WHERE (
                SELECT 1
                FROM json_each(track_frame)
                WHERE value = ?
            )
        """

        cursor = self.conn.cursor()
        cursor.execute(query, (f'{track_number}_{frame_number}',))
        track_frames = []

        for row in cursor.fetchall():
            track_frames.extend([(int(f.split('_')[0]), int(f.split('_')[1]))
                                 for f in json.loads(row[0].replace("'", '"'))])

        return set(track_frames)

    @cache
    def load_frame_db(self, filter_land=True):
        gdf = gpd.read_file(self.path, crs="EPSG:4326")

        if filter_land:
            gdf = gdf[gdf['land_ocean_flag'].isin(["water/land", "land"])]

        return gdf

    @cache
    def get_bounding_box_for_mgrs_set_id(self, mgrs_set_id):
        gdf = self.load_frame_db()

        if not len(gdf[gdf["mgrs_set_id"] == mgrs_set_id]):
            raise Exception(f"No MGRS burst database entry for {mgrs_set_id}")

        mgrs_entry = gdf[gdf["mgrs_set_id"] == mgrs_set_id].iloc[0]

        proj_src = f'EPSG:{mgrs_entry.EPSG}'
        proj_dst = gdf.crs
        transformer = Transformer.from_crs(proj_src, proj_dst)

        ymin, xmin = transformer.transform(
            xx=mgrs_entry.xmin,
            yy=mgrs_entry.ymin
        )
        ymax, xmax = transformer.transform(
            xx=mgrs_entry.xmax,
            yy=mgrs_entry.ymax
        )

        return [xmin, ymin, xmax, ymax]

    @cache
    def get_geojson_for_mgrs_set_id(self, mgrs_set_id):
        """Get the geojson representation of the MGRS tile set bounding polygon for a given MGRS tile set ID"""
        gdf = self.load_frame_db()

        if not len(gdf[gdf["mgrs_set_id"] == mgrs_set_id]):
            raise Exception(f"No MGRS burst database entry for {mgrs_set_id}")

        return json.loads(to_geojson(gdf.force_2d()[gdf["mgrs_set_id"] == mgrs_set_id].iloc[0]))  # We don't want this as a string

    @cache
    def get_max_track_frame(self):
        """Get the max track_frame. This will be the track frame after which cycle will increment"""

        query = f"SELECT track_number FROM {self.table_name} ORDER BY track_number DESC LIMIT 1"

        cursor = self.conn.cursor()
        cursor.execute(query)
        max_track = cursor.fetchone()[0]

        frames = []

        query = f"SELECT frames FROM {self.table_name} WHERE track_number = ?"

        cursor = self.conn.cursor()
        cursor.execute(query, (max_track,))
        for row in cursor.fetchall():
            frames.extend([int(frame) for frame in json.loads(row[0])])

        max_frame = max(frames)
        return f'{max_track}_{max_frame}'
