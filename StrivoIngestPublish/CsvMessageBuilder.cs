namespace StrivoIngestPublish;

/// <summary>
/// Provides CSV parsing and JSON message building utilities.
/// </summary>
internal static class CsvMessageBuilder
{
    /// <summary>
    /// Builds a JSON message payload from a CSV data line using default column names.
    /// Column names: Id, Message, Message2, Message3, …
    /// </summary>
    internal static string BuildMessage(string dataLine, string sourceBlobName)
    {
        var values = ParseCsvLine(dataLine);
        var headers = GenerateDefaultHeaders(values.Length);

        var fields = new List<string>(headers.Length + 1)
        {
            $"\"source\":\"{EscapeJson(sourceBlobName)}\""
        };

        for (int i = 0; i < headers.Length; i++)
        {
            string value = i < values.Length ? values[i] : string.Empty;
            fields.Add($"\"{headers[i]}\":\"{EscapeJson(value)}\"");
        }

        return "{" + string.Join(",", fields) + "}";
    }

    /// <summary>
    /// Returns default column names for a given column count.
    /// Index 0 → "Id", index 1 → "Message", index n ≥ 2 → "Message{n}".
    /// </summary>
    internal static string[] GenerateDefaultHeaders(int columnCount)
    {
        if (columnCount == 0)
        {
            return [];
        }

        var headers = new string[columnCount];
        for (int i = 0; i < columnCount; i++)
        {
            headers[i] = i switch
            {
                0 => "Id",
                1 => "Message",
                _ => $"Message{i}"
            };
        }

        return headers;
    }

    /// <summary>
    /// Parses a single CSV line, respecting RFC 4180 quoting rules.
    /// </summary>
    internal static string[] ParseCsvLine(string line)
    {
        var fields = new List<string>();
        bool inQuotes = false;
        var current = new System.Text.StringBuilder();

        for (int i = 0; i < line.Length; i++)
        {
            char c = line[i];
            if (c == '"')
            {
                if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                {
                    current.Append('"');
                    i++;
                }
                else
                {
                    inQuotes = !inQuotes;
                }
            }
            else if (c == ',' && !inQuotes)
            {
                fields.Add(current.ToString());
                current.Clear();
            }
            else
            {
                current.Append(c);
            }
        }

        fields.Add(current.ToString());
        return [.. fields];
    }

    internal static string EscapeJson(string value) =>
        value.Replace("\\", "\\\\").Replace("\"", "\\\"");
}
