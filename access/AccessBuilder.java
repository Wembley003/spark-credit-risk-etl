import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Construye un .accdb real de Microsoft Access desde los CSV y el
 * esquema que deja dump_access.py.
 *
 * <p>Access no se puede crear con Python en Linux, así que esto va en
 * Java con Jackcess, que escribe el formato ACE14 (Access 2010 en
 * adelante) sin necesitar ni Access ni Windows. El archivo que sale
 * lo abre Access tal cual: tablas con sus columnas, sus tipos y sus
 * filas, sin nada que importar a mano.
 *
 * <p>Uso: java AccessBuilder &lt;dir_access&gt; &lt;salida.accdb&gt;
 * donde dir_access tiene schema.txt y csv/*.csv.
 */
public class AccessBuilder {

    private static class Column {
        String name;
        String type;
        int length;
    }

    private static class TableSpec {
        String name;
        List<Column> columns = new ArrayList<>();
    }

    /** Un parser CSV chico pero honesto: comillas, comas dentro de
     * comillas y comillas dobles escapadas como "". Sin dependencias. */
    private static List<String> parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        current.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    current.append(c);
                }
            } else if (c == '"') {
                inQuotes = true;
            } else if (c == ',') {
                fields.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        fields.add(current.toString());
        return fields;
    }

    /** Lee un CSV entero (las celdas de estos proyectos no llevan
     * saltos de línea, así que línea a línea alcanza). */
    private static List<List<String>> readCsv(Path path) throws IOException {
        List<List<String>> rows = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(
                path, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                rows.add(parseCsvLine(line));
            }
        }
        return rows;
    }

    private static List<TableSpec> readSchema(Path path) throws IOException {
        List<TableSpec> tables = new ArrayList<>();
        TableSpec current = null;
        for (String raw : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            String line = raw.trim();
            if (line.isEmpty()) {
                continue;
            }
            String[] parts = line.split("\\|", -1);
            if (parts[0].equals("TABLE")) {
                current = new TableSpec();
                current.name = parts[1];
                tables.add(current);
            } else if (parts[0].equals("COL") && current != null) {
                Column col = new Column();
                col.name = parts[1];
                col.type = parts[2];
                col.length = parts.length > 3 && !parts[3].isEmpty()
                    ? Integer.parseInt(parts[3]) : 0;
                current.columns.add(col);
            }
        }
        return tables;
    }

    private static Object convert(String raw, Column col) {
        if (raw == null || raw.isEmpty()) {
            return null; // campo vacío en el CSV = nulo en Access
        }
        switch (col.type) {
            case "LONG":
                return Long.parseLong(raw);
            case "DOUBLE":
                return Double.parseDouble(raw);
            case "BOOLEAN":
                return raw.equals("1") || raw.equalsIgnoreCase("true");
            default:
                return raw;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println(
                "uso: AccessBuilder <dir_access> <salida.accdb>");
            System.exit(2);
        }
        Path accessDir = Path.of(args[0]);
        File outFile = new File(args[1]);
        List<TableSpec> tables = readSchema(accessDir.resolve("schema.txt"));

        if (outFile.exists()) {
            outFile.delete(); // Jackcess no sobreescribe: se borra antes
        }
        Database db = DatabaseBuilder.create(
            Database.FileFormat.V2010, outFile);
        try {
            for (TableSpec spec : tables) {
                TableBuilder builder = new TableBuilder(spec.name);
                for (Column col : spec.columns) {
                    ColumnBuilder cb;
                    switch (col.type) {
                        case "LONG":
                            cb = new ColumnBuilder(col.name, DataType.LONG);
                            break;
                        case "DOUBLE":
                            cb = new ColumnBuilder(col.name, DataType.DOUBLE);
                            break;
                        case "BOOLEAN":
                            cb = new ColumnBuilder(col.name, DataType.BOOLEAN);
                            break;
                        default:
                            // Ojo: Jackcess mide la longitud del texto en
                            // bytes (UTF-16, dos por carácter), así que
                            // para permitir N caracteres hay que pedir 2N.
                            // Comprobado: setLength(50) solo deja 25.
                            cb = new ColumnBuilder(col.name, DataType.TEXT)
                                .setLength(Math.max(2, col.length * 2));
                            break;
                    }
                    builder.addColumn(cb);
                }
                Table table = builder.toTable(db);
                List<List<String>> csv = readCsv(
                    accessDir.resolve("csv").resolve(spec.name + ".csv"));
                if (csv.isEmpty()) {
                    continue;
                }
                List<String> header = csv.get(0);
                int inserted = 0;
                for (int i = 1; i < csv.size(); i++) {
                    List<String> fields = csv.get(i);
                    Object[] values = new Object[spec.columns.size()];
                    for (int j = 0; j < spec.columns.size(); j++) {
                        String raw = j < fields.size() ? fields.get(j) : "";
                        // El encabezado del CSV manda en el orden, por si
                        // el esquema y el CSV difieren en algo.
                        int csvIndex = header.indexOf(spec.columns.get(j).name);
                        if (csvIndex >= 0 && csvIndex < fields.size()) {
                            raw = fields.get(csvIndex);
                        }
                        values[j] = convert(raw, spec.columns.get(j));
                    }
                    table.addRow(values);
                    inserted++;
                }
                System.out.println(spec.name + ": " + inserted + " filas");
            }
        } finally {
            db.close();
        }
        System.out.println("creado " + outFile.getAbsolutePath()
                           + " (" + outFile.length() + " bytes)");
    }
}
