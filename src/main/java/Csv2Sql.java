import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by lako on 14.02.2017.
 */
public class Csv2Sql {
    private static final Character CSV_DELIMITER = ';';


    public static void main(String[] args) {
        final String csvFileName = determineCsvFileName(args);
        final String tableName = determineTableName(args);
        final Integer idColumnIndex = determineIdColumnIndex(args);

        final List<Integer> foreignKeyColumnIndexes = determineForeignKeyColumnIndexes(args);

        final List<String> columnTypes = determineColumnTypes(args);

        Reader reader = null;
        Writer writer = null;

        try {
            reader = openCsvInputFile(csvFileName);
            final CSVParser csvParser = parseCsvFile(csvFileName, reader);

            writer = openSqlOutputFile(csvFileName);

            final String columnList = assembleColumnList(csvParser);
            final String insertStatementPrefix = assembleInsertStatementPrefix(tableName, columnList);

            assembleInsertStatements(csvParser, writer, insertStatementPrefix, idColumnIndex, columnTypes, foreignKeyColumnIndexes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeCsvInputFile(reader);
            closeSqlOutputFile(writer);
        }
    }

    private static List<Integer> determineForeignKeyColumnIndexes(String[] args) {
        final String foreignKeyColumnIndexes = args[3];
        return Arrays.stream(foreignKeyColumnIndexes.split(",")).map(Integer::parseInt).collect(Collectors.toList());
    }

    private static void closeSqlOutputFile(Writer writer) {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static void closeCsvInputFile(Reader reader) {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static BufferedReader openCsvInputFile(String csvFileName) throws FileNotFoundException {
        return new BufferedReader(new FileReader(csvFileName));
    }

    private static void assembleInsertStatements(final CSVParser csvParser, final Writer writer, final String insertStatementPrefix, final Integer idColumnIndex, final List<String> columnTypes, List<Integer> foreignKeyColumnIndexes) throws IOException {
        for (CSVRecord csvRecord : csvParser) {
            StringBuilder insertStatement = initializeInsertStatement(csvRecord, insertStatementPrefix, idColumnIndex);

            insertStatement = appendValuesToInsertStatement(csvRecord, insertStatement, columnTypes, foreignKeyColumnIndexes);

            writeInsertStatementToFile(insertStatement, writer);
        }
    }

    private static void writeInsertStatementToFile(final StringBuilder insertStatement, final Writer writer) throws IOException {
        writer.write(insertStatement.toString());
    }

    private static StringBuilder appendValuesToInsertStatement(final CSVRecord csvRecord, StringBuilder insertStatement, final List<String> columnTypes, final List<Integer> foreignKeyColumnIndexes) {
        for (int i = 0; i < csvRecord.size(); i++) {
            insertStatement = appendValueToStatement(csvRecord, insertStatement, i, columnTypes, foreignKeyColumnIndexes);
        }

        insertStatement.append(");\n");

        return insertStatement;
    }

    private static StringBuilder appendValueToStatement(final CSVRecord csvRecord, final StringBuilder insertStatement, final int i, final List<String> columnTypes, List<Integer> foreignKeyColumnIndexes) {
        final String value = determineValue(csvRecord, i, foreignKeyColumnIndexes);

        if (StringUtils.isBlank(value)) {
            insertStatement.append("NULL");
        } else {
            switch (columnTypes.get(i)) {
                case "t":
                    insertStatement.append("'").append(value.replace("'", "''")).append("'");
                    break;
                case "i":
                    insertStatement.append(value);
                    break;
                default:
                    throw new IllegalStateException("Unreachable code.");
            }
        }

        if (i + 1 != csvRecord.size()) {
            insertStatement.append(", ");
        }

        return insertStatement;
    }

    private static String determineValue(CSVRecord csvRecord, int i, List<Integer> foreignKeyColumnIndexes) {
        return isForeignKeyColumn(i, foreignKeyColumnIndexes) ? convertIdentifierToHex(csvRecord.get(i)) : csvRecord.get(i);
    }

    private static boolean isForeignKeyColumn(int i, List<Integer> foreignKeyColumnIndexes) {
        return foreignKeyColumnIndexes.contains(i);
    }

    private static StringBuilder initializeInsertStatement(final CSVRecord csvRecord, final String insertStatementPrefix, final Integer idColumnIndex) {
        StringBuilder insertStatement = new StringBuilder(insertStatementPrefix).append("'");
        insertStatement.append(convertIdentifierToHex(csvRecord.get(idColumnIndex)));
        insertStatement.append("', ");
        return insertStatement;
    }

    private static String convertIdentifierToHex(final String id) {
        if (StringUtils.isBlank(id)) {
            return null;
        } else {
            return String.format("%x", new BigInteger(1, id.getBytes()));
        }
    }

    private static String assembleInsertStatementPrefix(String tableName, String columnList) {
        return "INSERT INTO " + tableName + " (" + columnList + ") VALUES (";
    }

    private static String assembleColumnList(CSVParser csvParser) {
        return "id, " + csvParser.getHeaderMap().entrySet().stream().map(entry ->
                entry.getKey().toLowerCase().replace(' ', '_')).collect(Collectors.joining(", "));
    }

    private static List<String> determineColumnTypes(String[] args) {
        final String columnTypes = args[4];
        return Arrays.stream(columnTypes.split(",")).collect(Collectors.toList());
    }

    private static int determineIdColumnIndex(String[] args) {
        return Integer.parseInt(args[2]);
    }

    private static String determineCsvFileName(String... args) {
        return args[0];
    }

    private static String determineTableName(String... args) {
        return args[1];
    }

    private static Writer openSqlOutputFile(String csvFileName) throws IOException {
        final String sqlFileName = assembleSqlFileName(csvFileName);

        try {
            Files.delete(new File(sqlFileName).toPath());
        } catch (NoSuchFileException e) {
        }

        return new BufferedWriter(new FileWriter(sqlFileName));
    }

    private static String assembleSqlFileName(String csvFileName) {
        return StringUtils.substringBefore(csvFileName, ".csv") + ".sql";
    }

    private static CSVParser parseCsvFile(final String csvFileName, final Reader reader) throws IOException {

        return CSVFormat.EXCEL.withHeader(new String[]{}).withDelimiter(CSV_DELIMITER).parse(reader);
    }
}
