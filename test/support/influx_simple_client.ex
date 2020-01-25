defmodule TelemetryInfluxDB.Test.InfluxSimpleClient do
  defmodule V1 do
    def query(config, query) do
      url_encoded = URI.encode_query(%{"q" => query})

      path =
        config.host <>
          ":" <>
          :erlang.integer_to_binary(config.port) <>
          "/query?db=" <> config.db <> "&" <> url_encoded

      headers = authentication_header(config.username, config.password)
      process_response(HTTPoison.get(path, headers))
    end

    def post(config, query) do
      url_encoded = URI.encode_query(%{"q" => query})

      path =
        config.host <>
          ":" <>
          :erlang.integer_to_binary(config.port) <>
          "/query?db=" <> config.db <> "&" <> url_encoded

      headers = authentication_header(config.username, config.password)
      process_response(HTTPoison.post(path, "", headers))
    end

    defp process_response({:ok, %HTTPoison.Response{body: body}}) do
      {:ok, res} = Jason.decode(body)
      res
    end

    defp authentication_header(username, password) do
      %{"Authorization" => "Basic #{Base.encode64(username <> ":" <> password)}"}
    end
  end

  defmodule V2 do
    def query(config, query) do
      org_encoded = URI.encode_query(%{"org" => config.org})

      body =
        Jason.encode!(%{
          dialect: %{annotations: ["datatype"]},
          query: query
        })

      path =
        config.host <>
          ":" <>
          :erlang.integer_to_binary(config.port) <>
          "/api/v2/query?" <>
          org_encoded

      headers = headers(config)
      process_response(HTTPoison.post(path, body, headers))
    end

    def delete(%{bucket: bucket, org: org} = config, predicate) do
      # We're required to include a time range, so we create one that
      # should be large enough to capture all of the data while accounting
      # for any clock sync issues between the client and server.
      now = DateTime.utc_now()
      start = DateTime.add(now, -3600, :second)
      stop = DateTime.add(now, 3600, :second)

      query = URI.encode_query(%{bucket: bucket, org: org})

      body =
        Jason.encode!(%{
          predicate: predicate,
          start: DateTime.to_iso8601(start),
          stop: DateTime.to_iso8601(stop)
        })

      path =
        config.host <>
          ":" <>
          :erlang.integer_to_binary(config.port) <>
          "/api/v2/delete?" <>
          query

      headers = headers(config)
      process_response(HTTPoison.post(path, body, headers))
    end

    defp process_response({:ok, %HTTPoison.Response{body: body}}) do
      body
    end

    defp headers(config) do
      default_headers()
      |> Map.merge(authentication_header(config.token))
    end

    def default_headers() do
      %{
        "Accept" => "application/csv",
        "Content-type" => "application/json"
      }
    end

    defp authentication_header(token) do
      %{"Authorization" => "Token #{token}"}
    end
  end
end
