# TODO: Consider writing a v2 simple client instead of using this one

defmodule TelemetryInfluxDB.Test.InfluxSimpleClient do
  defmodule V2 do
    def query(config, query) do
      org_encoded = URI.encode_query(%{"org" => config.org})

      path =
        config.host <>
          ":" <>
          :erlang.integer_to_binary(config.port) <>
          "/query?" <>
          org_encoded

      headers = headers(config)
      process_response(HTTPoison.post(path, headers))
    end

    defp process_response({:ok, %HTTPoison.Response{body: body}}) do
      # TODO: add csv parser and parsing
      body
    end

    defp headers(config) do
      default_headers()
      |> Map.merge(authentication_header(config.token))
    end

    def default_headers() do
      %{
        "Accept" => "application/csv",
        "Content-type" => "application/vnd.flux"
      }
    end

    defp authentication_header(token) do
      %{"Authorization" => "Token #{token}"}
    end
  end

  defmodule V1 do
    def query(config, query) do
      url_encoded = URI.encode_query(%{"q" => query})

      # TODO: build proper URL for v2; maybe extract a helper in the code we can use both here
      # and in the http event_handler. Also needs to be a POST with a body.
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

      # TODO: build proper URL for v2; maybe extract a helper in the code we can use both here
      # and in the http event_handler. Also needs to put the query in the body as `predicate`.
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
end
