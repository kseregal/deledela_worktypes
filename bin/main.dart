import 'dart:io';
import 'package:yaml/yaml.dart';
import 'package:http/http.dart' as http;
import 'package:delidela_worktypes/src/delidela_worktypes.dart';

main(List<String> arguments) async {
  Map<String, dynamic> _elastic;
  Map<String, dynamic> _rethink;

  // Считывает данные конфигурации из файла и создает экземпляр класса [Config]
  Map<String, dynamic> config = await loadYaml(new File('config.yaml').readAsStringSync());

  for(String key in config.keys) {
    switch(key) {
      case 'elastic':
        _elastic = config[key];
        break;
      case 'rethink':
        _rethink = config[key];
        break;
    }
  }
  ExportRethinkDataToES exp =  new ExportRethinkDataToES(
      elasticHost: _elastic['url'],
      elasticLogin: _elastic['login'],
      elasticPassword: _elastic['password'],
      rethinkDbName: _rethink['dbName'],
      rethinkDbHost: _rethink['host'],
      rethinkDbPort: _rethink['port']
  );

  await exp.exportWorkType();

}
