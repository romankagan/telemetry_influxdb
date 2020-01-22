defmodule TelemetryInfluxDBTest do
  use ExUnit.Case, async: false
  alias TelemetryInfluxDB.Test.InfluxSimpleClient
  alias TelemetryInfluxDB.UDP
  import ExUnit.CaptureLog
  import Eventually

  @default_options %{
    db: "myinflux",
    username: "myuser",
    password: "mysecretpassword",
    host: "localhost",
    protocol: :udp,
    port: 8087
  }

  setup_all do
    token = File.read!(".token")

    {:ok, %{token: token}}
  end

  describe "Invalid reporter configuration - " do
    test "error log message is displayed for invalid influxdb credentials" do
      # given
      event = given_event_spec([:request, :failed])

      options =
        make_options(%{version: :v1, protocol: :http}, %{
          events: [event],
          username: "badguy",
          password: "wrongpass"
        })

      pid = start_reporter(options)
      testpid = self()

      :meck.new(TelemetryInfluxDB.HTTP.EventHandler, [:unstick, :passthrough])

      :meck.expect(TelemetryInfluxDB.HTTP.EventHandler, :send_event, fn q, b, h ->
        res = :meck.passthrough([q, b, h])
        send(testpid, :event_sent)
        res
      end)

      log =
        capture_log(fn ->
          # when
          :telemetry.execute([:request, :failed], %{"user" => "invalid", "password" => "invalid"})
          assert_receive :event_sent, 500
        end)

      ## then
      assert log =~ "Failed to push data to InfluxDB. Invalid credentials"
      stop_reporter(pid)
      :meck.unload(TelemetryInfluxDB.HTTP.EventHandler)
    end

    test "error log message is displayed for invalid influxdb database" do
      # given
      event = given_event_spec([:users, :count])

      options =
        make_options(
          %{
            version: :v1,
            protocol: :http
          },
          %{events: [event], db: "yy_postgres"}
        )

      pid = start_reporter(options)
      testpid = self()
      :meck.new(TelemetryInfluxDB.HTTP.EventHandler, [:unstick, :passthrough])

      :meck.expect(TelemetryInfluxDB.HTTP.EventHandler, :send_event, fn q, b, h ->
        res = :meck.passthrough([q, b, h])
        send(testpid, :event_sent)
        res
      end)

      log =
        capture_log(fn ->
          # when
          :telemetry.execute([:users, :count], %{"value" => "30"})
          assert_receive :event_sent, 200
        end)

      # then
      assert log =~ "Failed to push data to InfluxDB. Invalid credentials"
      stop_reporter(pid)
      :meck.unload(TelemetryInfluxDB.HTTP.EventHandler)
    end

    test "error log message is displayed for missing db for HTTP" do
      assert_raise(
        ArgumentError,
        "for http protocol you need to specify :db field",
        fn ->
          @default_options
          |> Map.delete(:db)
          |> Map.put(:protocol, :http)
          |> Map.put(:events, [given_event_spec([:missing, :db])])
          |> start_reporter()
        end
      )
    end

    test "error message is displayed for missing bucket in v2 options", %{token: token} do
      assert_raise(
        ArgumentError,
        "for InfluxDB v2 you need to specify :bucket, :org, and :token fields",
        fn ->
          @default_options
          |> be_v2(token)
          |> Map.delete(:bucket)
          |> Map.put(:events, [given_event_spec([:missing, :bucket])])
          |> start_reporter()
        end
      )
    end

    test "error message is displayed for missing org in v2 options", %{token: token} do
      assert_raise(
        ArgumentError,
        "for InfluxDB v2 you need to specify :bucket, :org, and :token fields",
        fn ->
          @default_options
          |> be_v2(token)
          |> Map.delete(:org)
          |> Map.put(:events, [given_event_spec([:missing, :org])])
          |> start_reporter()
        end
      )
    end

    test "error message is displayed for invalid version" do
      assert_raise(
        ArgumentError,
        "version must be :v1 or :v2",
        fn ->
          @default_options
          |> Map.put(:version, :bad_version)
          |> Map.put(:events, [given_event_spec([:invalid, :version])])
          |> start_reporter()
        end
      )
    end

    test "error message is displayed for missing token in v2 options" do
      assert_raise(
        ArgumentError,
        "for InfluxDB v2 you need to specify :bucket, :org, and :token fields",
        fn ->
          @default_options
          |> be_v2()
          |> Map.delete(:token)
          |> Map.put(:events, [given_event_spec([:missing, :token])])
          |> start_reporter()
        end
      )
    end

    test "error message is displayed when specifying udp protocol with v2 options" do
      assert_raise(
        ArgumentError,
        "the udp protocol is not currently supported for InfluxDB v2; please use http instead",
        fn ->
          @default_options
          |> be_v2()
          |> Map.put(:protocol, :udp)
          |> Map.put(:events, [given_event_spec([:v2, :udp])])
          |> start_reporter()
        end
      )
    end
  end

  describe "Events reported - " do
    # TODO: Iterate through v1 http, v1 udp, v2 http
    for {version, protocol} <- [{:v1, :http}, {:v1, :udp}] do
      @tag protocol: protocol
      @tag version: version
      test "event is reported when specified by its name for #{version} #{protocol} API",
           context do
        ## given
        event = given_event_spec([:requests, :failed])
        options = make_options(context, %{events: [event]})
        pid = start_reporter(options)

        ## when
        :telemetry.execute([:requests, :failed], %{"reason" => "timeout", "retries" => 3})

        ## then
        assert_reported("requests.failed", %{"reason" => "timeout", "retries" => 3})

        ## cleanup
        clear_series("requests.failed")
        stop_reporter(pid)
      end

      @tag version: version
      @tag protocol: protocol
      test "event is reported with correct data types for #{version} #{protocol} API", context do
        ## given
        event = given_event_spec([:calls, :failed])
        options = make_options(context, %{events: [event]})
        pid = start_reporter(options)

        ## when
        :telemetry.execute([:calls, :failed], %{
          "int" => 4,
          "string_int" => "3",
          "float" => 0.34,
          "string" => "random",
          "boolean" => true
        })

        ## then
        assert_reported("calls.failed", %{
          "int" => 4,
          "string_int" => "3",
          "float" => 0.34,
          "string" => "random",
          "boolean" => true
        })

        ## cleanup
        clear_series("calls.failed")
        stop_reporter(pid)
      end

      @tag version: version
      @tag protocol: protocol
      test "only specified events are reported for #{version} #{protocol} API", context do
        ## given
        event1 = given_event_spec([:event, :one])
        event2 = given_event_spec([:event, :two])
        event3 = given_event_spec([:event, :three])
        options = make_options(context, %{events: [event1, event2, event3]})
        pid = start_reporter(options)
        ## when
        :telemetry.execute([:event, :one], %{"value" => 1})
        assert_reported("event.one", %{"value" => 1})

        :telemetry.execute([:event, :two], %{"value" => 2})
        assert_reported("event.two", %{"value" => 2})

        :telemetry.execute([:event, :other], %{"value" => "?"})

        ## then
        refute_reported("event.other")

        ## cleanup
        clear_series("event.one")
        clear_series("event.two")
        clear_series("event.other")
        stop_reporter(pid)
      end

      @tag version: version
      @tag protocol: protocol
      test "events are reported with global pre-defined tags for #{version} #{protocol} API",
           context do
        ## given
        event = given_event_spec([:memory, :leak])

        options =
          make_options(context, %{
            events: [event],
            tags: %{region: :eu_central, time_zone: :cest}
          })

        pid = start_reporter(options)

        ## when
        :telemetry.execute([:memory, :leak], %{"memory_leaked" => 100})

        ## then
        assert_reported("memory.leak", %{"memory_leaked" => 100}, %{
          "region" => "\"eu_central\"",
          "time_zone" => "\"cest\""
        })

        ## cleanup
        clear_series("memory.leak")
        stop_reporter(pid)
      end

      @tag version: version
      @tag protocol: protocol
      test "events are reported with event-specific tags for #{version} #{protocol} API",
           context do
        ## given
        event = given_event_spec([:system, :crash])
        options = make_options(context, %{events: [event], tags: %{}})
        pid = start_reporter(options)

        ## when
        :telemetry.execute([:system, :crash], %{"node_id" => "a3"}, %{tags: %{priority: :high}})

        ## then
        assert_reported("system.crash", %{"node_id" => "a3"}, %{
          "priority" => "\"high\""
        })

        ## cleanup
        clear_series("system.crash")
        stop_reporter(pid)
      end

      @tag version: version
      @tag protocol: protocol
      test "events are reported with special characters for #{version} #{protocol} API",
           context do
        ## given
        event1 = given_event_spec([:event, :special1])
        event2 = given_event_spec([:event, :special2])
        options = make_options(context, %{events: [event1, event2], tags: %{}})
        pid = start_reporter(options)

        ## when
        :telemetry.execute([:event, :special1], %{"equal_sign" => "a=b"}, %{
          tags: %{priority: "hig\"h"}
        })

        :telemetry.execute([:event, :special2], %{"comma_space" => "a,b c"}, %{tags: %{}})

        ## then
        assert_reported("event.special1", %{"equal_sign" => "a\\\=b"}, %{
          "priority" => "\"hig\\\\\"h\""
        })

        assert_reported("event.special2", %{"comma_space" => "a\\,b\\ c"}, %{})

        ## cleanup
        clear_series("event.special1")
        clear_series("event.special2")
        stop_reporter(pid)
      end

      @tag version: version
      @tag protocol: protocol
      test "events are detached after stopping reporter for #{version} #{protocol} API",
           context do
        ## given
        event_old = given_event_spec([:old, :event])
        event_new = given_event_spec([:new, :event])
        options = make_options(context, %{events: [event_old, event_new]})
        pid = start_reporter(options)

        :telemetry.execute([:old, :event], %{"value" => 1})
        assert_reported("old.event", %{"value" => 1})

        ## when
        TelemetryInfluxDB.stop(pid)
        :telemetry.execute([:new, :event], %{"value" => 2})

        ## then
        refute_reported("new.event")

        ## cleanup
        clear_series("old.event")
      end

      @tag :capture_log
      @tag version: version
      @tag protocol: protocol
      test "events are not reported when reporter receives an exit signal for #{version} #{
             protocol
           } API",
           context do
        ## given
        event_first = given_event_spec([:first, :event])
        event_second = given_event_spec([:second, :event])
        options = make_options(context, %{events: [event_first, event_second]})
        pid = start_reporter(options)

        Process.unlink(pid)
        {:links, child_pids} = :erlang.process_info(pid, :links)

        # Make sure that event handlers are detached even if non-parent process sends an exit signal.

        spawn(fn -> Process.exit(pid, :kill) end)
        wait_processes_to_die(child_pids ++ [pid])

        assert :telemetry.list_handlers([:first, :event]) == []
        assert :telemetry.list_handlers([:second, :event]) == []

        :telemetry.execute([:first, :event], %{})
        :telemetry.execute([:second, :event], %{})

        refute_reported("first.event")
        refute_reported("second.event")
      end

      @tag version: version
      @tag protocol: protocol
      test "events are reported from two independent reporters for #{version} #{protocol} API",
           context do
        ## given
        event1 = given_event_spec([:servers1, :down])
        event2 = given_event_spec([:servers2, :down])

        options =
          make_options(context, %{
            events: [event1],
            tags: %{region: :eu_central, time_zone: :cest},
            reporter_name: "eu"
          })

        pid1 = start_reporter(options)

        options =
          make_options(context, %{
            events: [event2],
            tags: %{region: :asia, time_zone: :other},
            reporter_name: "asia"
          })

        pid2 = start_reporter(options)

        ## when
        :telemetry.execute([:servers1, :down], %{"panic?" => "yes"})
        :telemetry.execute([:servers2, :down], %{"panic?" => "yes"})

        ## then
        assert_reported("servers1.down", %{"panic?" => "yes"}, %{
          "region" => "\"eu_central\"",
          "time_zone" => "\"cest\""
        })

        assert_reported("servers2.down", %{"panic?" => "yes"}, %{
          "region" => "\"asia\"",
          "time_zone" => "\"other\""
        })

        ## cleanup
        clear_series("servers1.down")
        clear_series("servers2.down")
        stop_reporter(pid1)
        stop_reporter(pid2)
      end
    end
  end

  @tag :capture_log
  test "notifying a UDP error and fetching a socket returns a new socket" do
    event = given_event_spec([:some, :event3])
    options = make_options(%{version: :v1, protocol: :udp}, %{events: [event], tags: %{}})
    start_reporter(options)
    udp = UDP.Connector.get_udp("default")
    Process.exit(udp.socket, :kill)

    assert eventually(fn ->
             new_udp = UDP.Connector.get_udp("default")
             new_udp != udp
           end)
  end

  test "events are not reported when reporter is shut down by its supervisor" do
    event_first = given_event_spec([:first, :event])
    event_second = given_event_spec([:second, :event])
    child_opts = [Map.to_list(@default_options) ++ [events: [event_first, event_second]]]

    {:ok, supervisor} =
      Supervisor.start_link(
        [
          Supervisor.Spec.worker(TelemetryInfluxDB, child_opts)
        ],
        strategy: :one_for_one
      )

    Process.unlink(supervisor)

    Supervisor.stop(supervisor, :shutdown)

    assert :telemetry.list_handlers([:first, :event]) == []
    assert :telemetry.list_handlers([:second, :event]) == []

    :telemetry.execute([:first, :event], %{})
    :telemetry.execute([:second, :event], %{})

    refute_reported("first.event")
    refute_reported("second.event")
  end

  defp given_event_spec(name) do
    %{name: name}
  end

  defp refute_reported(name, config \\ @default_options) do
    # TODO: Write a flux version of this for v2
    q = "SELECT * FROM \"" <> name <> "\";"
    res = InfluxSimpleClient.query(config, q)
    assert %{"results" => [%{"statement_id" => 0}]} == res
  end

  # TODO: Write a flux version of this for v2
  defp assert_reported(name, values, tags \\ %{}, config \\ @default_options) do
    assert record =
             eventually(fn ->
               q = "SELECT * FROM \"" <> name <> "\";"
               res = InfluxSimpleClient.query(config, q)

               with [inner_map] <- res["results"],
                    [record] <- inner_map["series"] do
                 record
               else
                 _ -> false
               end
             end)

    assert record["name"] == name
    assert record["columns"] == ["time"] ++ Map.keys(values) ++ Map.keys(tags)
    map_vals = Map.values(values)
    map_tag_vals = Map.values(tags)
    all_vals = map_vals ++ map_tag_vals

    assert [[_ | tag_and_fields]] = record["values"]
    assert tag_and_fields == all_vals
  end

  defp clear_series(name, config \\ @default_options) do
    # TODO: Write a flux version of this for v2
    q = "DROP SERIES FROM \"" <> name <> "\";"
    InfluxSimpleClient.post(config, q)

    eventually(fn ->
      q = "SELECT * FROM \"" <> name <> "\";"
      InfluxSimpleClient.query(config, q) == %{"results" => [%{"statement_id" => 0}]}
    end)
  end

  # TODO: Add a v2 version of this
  defp make_options(%{version: :v1, protocol: :udp}, overrides) do
    @default_options
    |> Map.delete(:db)
    |> Map.merge(%{protocol: :udp, port: 8089})
    |> Map.merge(overrides)
  end

  defp make_options(%{version: :v1, protocol: :http}, overrides) do
    @default_options
    |> Map.merge(%{protocol: :http, port: 8087})
    |> Map.merge(overrides)
  end

  defp start_reporter(options) do
    {:ok, pid} = TelemetryInfluxDB.start_link(options)
    pid
  end

  defp be_v2(options) do
    options
    |> Map.delete(:db)
    |> Map.merge(%{
      version: :v2,
      protocol: :http,
      port: 9999,
      bucket: "myinflux",
      org: "myorg"
    })
  end

  defp be_v2(options, token) do
    options
    |> be_v2()
    |> Map.merge(%{token: token})
  end

  defp wait_processes_to_die(pids) do
    eventually(fn -> Enum.all?(pids, fn p -> not Process.alive?(p) end) end)
  end

  defp stop_reporter(pid) do
    TelemetryInfluxDB.stop(pid)
  end
end
