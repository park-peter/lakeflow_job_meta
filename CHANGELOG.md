# Changelog

## [0.1.0] 2025-11-03 - Initial Release

### Added

#### Core Framework
- ✅ Modular code structure (`src/` directory)
- ✅ Support for multiple task types:
  - Notebook tasks
  - Python wheels tasks
  - Scala tasks
  - SQL query tasks (w/ support for inline SQL)
  - SQL file tasks (SQL from files)
  - DBT Tasks
- ✅ Dynamic job generation from metadata
- ✅ Job lifecycle management (create/update/track)
- ✅ Execution order and dependency management
- ✅ Comprehensive error handling and logging

#### Documentation
- ✅ README with quick start guide
- ✅ Code review documentation
- ✅ Architecture analysis


### Folder Structure

```
.
├── lakeflow_jobs_meta/   # Main package
├── examples/              # Example files and templates
│   ├── orchestrator_example.ipynb  # Orchestrator example
│   ├── sql_file_task/    # SQL file task examples
│   ├── notebook_task/   # Notebook task examples
│   └── metadata_examples.yaml  # Example metadata
├── docs/                 # Documentation
└── tests/               # Tests
```

### Next Steps (Planned)

- [ ] Python Script Tasks
- [ ] Enhanced error handling with retry logic
- [ ] Execution monitoring dashboard
- [ ] Data quality framework