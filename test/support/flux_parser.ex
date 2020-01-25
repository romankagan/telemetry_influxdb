defmodule TelemetryInfluxDB.Test.FluxParser do
  alias NimbleCSV.RFC4180, as: CSV

  @column_types %{
    "boolean" => :boolean,
    "double" => :double,
    "string" => :string,
    "long" => :long,
    "unsignedLong" => :unsigned_long,
    "dateTime:RFC3339" => :datetime
  }

  def parse_tables(csv) do
    csv
    |> parse_chunks()
    |> Enum.map(fn chunk ->
      table_data =
        chunk
        |> extract_table_text()
        |> parse_csv()

      annotation_data =
        chunk
        |> extract_annotation_text()
        |> parse_csv()

      case length(table_data) do
        0 ->
          %{}

        _ ->
          [column_names | table_rows] = table_data
          column_types = annotation_data |> get_column_types()
          parse_columns(table_rows, column_names, column_types)
      end
    end)
    |> handle_error()
  end

  def get_column_types(annotation_data) do
    col_types_index =
      annotation_data
      |> Enum.find_index(fn a -> Enum.at(a, 0) == "#datatype" end)

    annotation_data
    |> Enum.at(col_types_index)
  end

  def parse_columns(
        table,
        [_datatype | column_names],
        [_ | column_types]
      ) do
    column_data = column_defaults(column_names)
    indexes = column_info_by_index(column_names, column_types)

    table
    |> Enum.reduce(column_data, fn [_default | row], columns ->
      row
      |> Enum.with_index()
      |> Enum.reduce(columns, fn {raw_value, index}, row_updates ->
        {name, type} = Map.get(indexes, index)

        value = parse_value(raw_value, type)

        data =
          row_updates
          |> Map.get(name)
          |> Enum.concat([value])

        Map.put(row_updates, name, data)
      end)
    end)
  end

  def column_defaults(column_names) do
    column_names
    |> Enum.reduce(%{}, fn name, acc -> Map.put(acc, name, []) end)
  end

  def column_info_by_index(column_names, column_types) do
    column_names
    |> Enum.with_index()
    |> Enum.reduce(%{}, fn {name, index}, acc ->
      raw_type = column_types |> Enum.at(index)
      type = @column_types |> Map.get(raw_type)

      Map.put(acc, index, {name, type})
    end)
  end

  def extract_table_text(table_text) do
    table_text
    |> String.split("\n")
    |> Enum.filter(fn line -> !String.starts_with?(line, "#") end)
    |> Enum.join("\n")
    |> String.trim()
  end

  def extract_annotation_text(table_text) do
    table_text
    |> String.split("\n")
    |> Enum.filter(fn line -> String.starts_with?(line, "#") end)
    |> Enum.join("\n")
    |> String.trim()
  end

  def parse_chunks(csv) do
    csv
    |> String.trim()
    |> String.split(~r/\n\s*\n/)
  end

  def parse_value("null", _type), do: nil

  def parse_value("true", :boolean), do: true
  def parse_value("false", :boolean), do: false

  def parse_value(string, :string), do: string

  def parse_value("NaN", :double), do: NaN

  def parse_value(string, :double) do
    case Float.parse(string) do
      {value, _} -> value
      :error -> raise ArgumentError, "invalid double argument: '#{string}'"
    end
  end

  def parse_value(datetime, :datetime) do
    case DateTime.from_iso8601(datetime) do
      {:ok, datetime, _offset} -> %{datetime | microsecond: {0, 6}}
      {:error, _} -> raise ArgumentError, "invalid datetime argument: '#{datetime}'"
    end
  end

  def parse_value(raw, :unsigned_long) do
    value = parse_integer(raw)

    if value < 0 do
      raise ArgumentError, message: "invalid unsigned_long argument: '#{value}'"
    end

    value
  end

  def parse_value(raw, :long), do: parse_integer(raw)

  defp parse_integer("NaN"), do: NaN

  defp parse_integer(raw) do
    {value, _} = Integer.parse(raw, 10)

    value
  end

  def parse_csv(csv) do
    CSV.parse_string(csv, skip_headers: false)
  end

  defp handle_error(tables) do
    error_table =
      tables
      |> Enum.find(fn table ->
        Map.has_key?(table, "error")
      end)

    if error_table != nil do
      {:error, tables}
    else
      {:ok, tables}
    end
  end
end
