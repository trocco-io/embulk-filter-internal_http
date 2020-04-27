Embulk::JavaPlugin.register_filter(
  "internal_http", "org.embulk.filter.internal_http.InternalHttpFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
