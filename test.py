import subprocess


def test_extract():
    """Test the extract() function."""
    result = subprocess.run(
        ["python", "main.py", "extract"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0
    assert "Extracting data..." in result.stdout


def test_load():
    """Test the transform_load() function."""
    result = subprocess.run(
        ["python", "main.py", "load"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0
    assert "Transforming and loading data..." in result.stdout


def test_query():
    """Test queryData() with a complex SQL query for player statistics."""
    query_string = """
    WITH player_stats AS (
        SELECT '2024' AS season,
            p.PLAYER_NAME AS player,
            r.TEAM AS team,
            r.OPP AS opponent,
            p.PROJ_FPTS AS projected_points
        FROM drm85_mp6.drm85_wr_points p
        JOIN drm85_mp6.drm85_wr_ranking r ON p.PLAYER_NAME = r.PLAYER_NAME
        WHERE p.PROJ_FPTS IS NOT NULL
    ),
    team_player_stats AS (
        SELECT team,
            player,
            AVG(projected_points) AS avg_projected_points
        FROM player_stats
        GROUP BY team, player
    )
    
    SELECT team, player, avg_projected_points
    FROM team_player_stats
    ORDER BY avg_projected_points DESC
    LIMIT 10;
    """

    try:
        subprocess.run(
            ["python3", "main.py", "query", query_string],
            capture_output=True,
            text=True,
            check=True,  # Raise an error if the command fails
        )

        print("Query Test Passed!")

    except subprocess.CalledProcessError as e:
        print(f"Query failed with return code {e.returncode}")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)


if __name__ == "__main__":
    test_extract()
    test_load()
    test_query()
