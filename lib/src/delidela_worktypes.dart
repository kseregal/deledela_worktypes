import 'dart:core';
import 'dart:async';
import 'dart:convert';
import 'package:rethinkdb_driver2/rethinkdb_driver2.dart';
import 'package:elastic_rest_api/elastic_rest.dart';

class ExportRethinkDataToES {
  /// Параметры соединения с базой ElastiSearch.
  /// Имя хоста ES
  String _elasticHost;
  /// Логин для соединения с ES
  String _elasticLogin;
  /// Пароль для соединения с ES
  String _elasticPassword;
  /// Имя базы в RethinkDb
  String _rethinkDbName;
  /// Имя хоста RethinkDb
  String _rethinkDbHost;
  /// Порт RethinkDb
  int _rethinkDbPort;
  /// Объект реазизующий взамодействие с REST API базы данных Elastic Search.
  ElasticApi _esApi;
  /// Объект управления  соединениями с базой RethinkDB
  Rethinkdb _r;
  /// Соединение с базой RethinkDb
  Connection _rethinkConn;

  ExportRethinkDataToES({
    String elasticHost,
    String elasticLogin,
    String elasticPassword,
    String rethinkDbName,
    String rethinkDbHost,
    int rethinkDbPort: 28015
  }) : _elasticHost = elasticHost,
        _elasticLogin = elasticLogin,
        _elasticPassword = elasticPassword,
        _rethinkDbName = rethinkDbName,
        _rethinkDbHost = rethinkDbHost,
        _rethinkDbPort = rethinkDbPort;

  exportWorkType () async {
    print('export');
    //r.db("delidela").table("workTypes");

    try {
      /// Создаем API для работы с базой Elastic Search.
      _esApi = new ElasticApi(
          _elasticHost,
          _elasticLogin,
          _elasticPassword
      );

      /// Создаем соединение с базой данных RethinkDb.
      _r = new Rethinkdb();
      _rethinkConn = await
      _r.connect(
          db: _rethinkDbName,
          host: _rethinkDbHost,
          port: _rethinkDbPort
      );



      List<Map<String, dynamic>> result = await _r.table("workTypes").coerceTo('array').run(_rethinkConn);
      result.forEach((workTypeData) async {
        (workTypeData["subtypes"] as List<String>).forEach((String subType) async {
          Map<String, dynamic> workTypesForExport = {
            "workTypeSubtype": "${workTypeData["type"]} ${subType}",
            "workType": workTypeData["type"],
            "subType": subType
          };
          print(workTypesForExport);
          await _esApi.elasticRequest.post( "contractors_worktype/doc/", JSON.encode(workTypesForExport) );
        });

      });
    } catch (e, trace) {
      print(e);
      print(trace);
    }
  }

}