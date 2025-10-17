#ifndef SQLPARSER_IMPORT_EXPORT_OPTIONS_H
#define SQLPARSER_IMPORT_EXPORT_OPTIONS_H

namespace hsql {

enum ImportType {
  kImportCSV,
  kImportTbl,
  kImportBinary,
  kImportAuto
};

struct ImportExportOptions {
  ImportExportOptions();
  ~ImportExportOptions();

  ImportType format;
  char* encoding;
};

}

#endif
