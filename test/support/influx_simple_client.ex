# TODO: Consider writing a v2 simple client instead of using this one

defmodule TelemetryInfluxDB.Test.InfluxSimpleClient do
  alias __MODULE__.{V1, V2}

  def query(%{version: :v1} = config, query) do
    V1.query(config, query)
  end

  def query(%{version: :v2} = config, query) do
    V2.query(config, query)
  end

  def post(%{version: :v1} = config, query) do
    V1.post(config, query)
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

  defmodule V2 do
    def query(config, query) do
      org_encoded = URI.encode_query(%{"org" => config.org})
      body = Jason.encode!(%{query: query})

      path =
        config.host <>
          ":" <>
          :erlang.integer_to_binary(config.port) <>
          "/api/v2/query?" <>
          org_encoded

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
