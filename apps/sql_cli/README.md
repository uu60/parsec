# SQL command-line demo

This directory contains the optional SQL-like application built on Parsec's secure relational algorithms.
It owns the SQL parser, command dispatch, local socket service, interactive client, and demo entry points.

The reusable database structures and secure operators remain in `db/` and are exposed through the
`db_core` CMake target. Application code in this directory depends on `db_core`; the dependency does not
flow in the opposite direction.

Configure and build the demo with:

```bash
cmake --preset release
cmake --build --preset release --target db db_client db_debugSql
```

To build only the database algorithms, disable the application layer:

```bash
cmake -S . -B build -G Ninja \
  -DPARSEC_BUILD_DATABASE=ON \
  -DPARSEC_BUILD_SQL_CLI=OFF
```

The `db` executable runs the three-rank service and `db_client` provides the local interactive prompt.
The client loads `client/precmd` before accepting interactive commands.
