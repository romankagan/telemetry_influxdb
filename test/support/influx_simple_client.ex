# TODO: Consider writing a v2 simple client instead of using this one

defmodule TelemetryInfluxDB.Test.InfluxSimpleClient do
  def query(config, query) do
    url_encoded = URI.encode_query(%{"q" => query})

    # TODO: build proper URL for v2; maybe extract a helper in the code we can use both here
    # and in the http event_handler. Also needs to be a POST with a body.
    path =
      config.host <>
        ":" <>
        :erlang.integer_to_binary(config.port) <> "/query?db=" <> config.db <> "&" <> url_encoded

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
        :erlang.integer_to_binary(config.port) <> "/query?db=" <> config.db <> "&" <> url_encoded

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
