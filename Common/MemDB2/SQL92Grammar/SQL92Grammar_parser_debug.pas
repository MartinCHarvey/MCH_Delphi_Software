unit SQL92Grammar_parser_debug;

interface

uses SysUtils, Classes;

function GetStateDebug(state: integer):TStringList;

implementation

const
  ListInfo: array [0..27643] of string = (
    '',
    'state 0:',
    '',
    '	$accept : _ SQL92Grammar $end',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	left_paren	shift 68',
    '	underscore	shift 69',
    '	_ALTER	shift 70',
    '	_COMMIT	shift 71',
    '	_CONNECT	shift 72',
    '	_CREATE	shift 73',
    '	_DECLARE	shift 74',
    '	_DELETE	shift 75',
    '	_DISCONNECT	shift 76',
    '	_DROP	shift 77',
    '	_GRANT	shift 78',
    '	_INSERT	shift 79',
    '	_MODULE	shift 80',
    '	_REVOKE	shift 81',
    '	_ROLLBACK	shift 82',
    '	_SELECT	shift 83',
    '	_SET	shift 84',
    '	_TABLE	shift 85',
    '	_UPDATE	shift 86',
    '	_VALUES	shift 87',
    '	.	error',
    '',
    '	sql_input	goto 1',
    '	sql_script	goto 2',
    '	sql_statement	goto 3',
    '	direct_select_statement__multiple_rows	goto 4',
    '	direct_implementation_defined_statement	goto 5',
    '	direct_SQL_data_statement	goto 6',
    '	direct_SQL_statement	goto 7',
    '	set_local_time_zone_statement	goto 8',
    '	set_session_authorization_identifier_statement	goto 9',
    '	set_names_statement	goto 10',
    '	set_schema_statement	goto 11',
    '	set_catalog_statement	goto 12',
    '	disconnect_statement	goto 13',
    '	set_connection_statement	goto 14',
    '	connect_statement	goto 15',
    '	rollback_statement	goto 16',
    '	commit_statement	goto 17',
    '	set_constraints_mode_statement	goto 18',
    '	set_transaction_statement	goto 19',
    '	update_statement__searched	goto 20',
    '	insert_statement	goto 21',
    '	delete_statement__searched	goto 22',
    '	drop_assertion_statement	goto 23',
    '	drop_translation_statement	goto 24',
    '	drop_collation_statement	goto 25',
    '	drop_character_set_statement	goto 26',
    '	drop_domain_statement	goto 27',
    '	alter_domain_statement	goto 28',
    '	revoke_statement	goto 29',
    '	drop_view_statement	goto 30',
    '	drop_table_statement	goto 31',
    '	alter_table_statement	goto 32',
    '	drop_schema_statement	goto 33',
    '	assertion_definition	goto 34',
    '	translation_definition	goto 35',
    '	collation_definition	goto 36',
    '	character_set_definition	goto 37',
    '	domain_definition	goto 38',
    '	grant_statement	goto 39',
    '	view_definition	goto 40',
    '	table_definition	goto 41',
    '	schema_definition	goto 42',
    '	SQL_schema_manipulation_statement	goto 43',
    '	SQL_schema_definition_statement	goto 44',
    '	SQL_session_statement	goto 45',
    '	SQL_connection_statement	goto 46',
    '	SQL_transaction_statement	goto 47',
    '	SQL_schema_statement	goto 48',
    '	explicit_table	goto 49',
    '	table_value_constructor	goto 50',
    '	query_specification	goto 51',
    '	table_subquery	goto 52',
    '	simple_table	goto 53',
    '	non_join_query_primary	goto 54',
    '	query_term	goto 55',
    '	non_join_query_term	goto 56',
    '	query_expression	goto 57',
    '	temporary_table_declaration	goto 58',
    '	module_name_clause	goto 59',
    '	module	goto 60',
    '	actual_identifier	goto 61',
    '	identifier	goto 62',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '	SQL92Grammar	goto 65',
    '',
    'state 1:',
    '',
    '	SQL92Grammar : sql_input _	(904)',
    '',
    '	.	reduce 904',
    '',
    'state 2:',
    '',
    '	sql_script : sql_script _ sql_statement',
    '	sql_input : sql_script _	(902)',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	left_paren	shift 68',
    '	underscore	shift 69',
    '	_ALTER	shift 70',
    '	_COMMIT	shift 71',
    '	_CONNECT	shift 72',
    '	_CREATE	shift 73',
    '	_DECLARE	shift 74',
    '	_DELETE	shift 75',
    '	_DISCONNECT	shift 76',
    '	_DROP	shift 77',
    '	_GRANT	shift 78',
    '	_INSERT	shift 79',
    '	_REVOKE	shift 81',
    '	_ROLLBACK	shift 82',
    '	_SELECT	shift 83',
    '	_SET	shift 84',
    '	_TABLE	shift 85',
    '	_UPDATE	shift 86',
    '	_VALUES	shift 87',
    '	$end	reduce 902',
    '	.	error',
    '',
    '	sql_statement	goto 88',
    '	direct_select_statement__multiple_rows	goto 4',
    '	direct_implementation_defined_statement	goto 5',
    '	direct_SQL_data_statement	goto 6',
    '	direct_SQL_statement	goto 7',
    '	set_local_time_zone_statement	goto 8',
    '	set_session_authorization_identifier_statement	goto 9',
    '	set_names_statement	goto 10',
    '	set_schema_statement	goto 11',
    '	set_catalog_statement	goto 12',
    '	disconnect_statement	goto 13',
    '	set_connection_statement	goto 14',
    '	connect_statement	goto 15',
    '	rollback_statement	goto 16',
    '	commit_statement	goto 17',
    '	set_constraints_mode_statement	goto 18',
    '	set_transaction_statement	goto 19',
    '	update_statement__searched	goto 20',
    '	insert_statement	goto 21',
    '	delete_statement__searched	goto 22',
    '	drop_assertion_statement	goto 23',
    '	drop_translation_statement	goto 24',
    '	drop_collation_statement	goto 25',
    '	drop_character_set_statement	goto 26',
    '	drop_domain_statement	goto 27',
    '	alter_domain_statement	goto 28',
    '	revoke_statement	goto 29',
    '	drop_view_statement	goto 30',
    '	drop_table_statement	goto 31',
    '	alter_table_statement	goto 32',
    '	drop_schema_statement	goto 33',
    '	assertion_definition	goto 34',
    '	translation_definition	goto 35',
    '	collation_definition	goto 36',
    '	character_set_definition	goto 37',
    '	domain_definition	goto 38',
    '	grant_statement	goto 39',
    '	view_definition	goto 40',
    '	table_definition	goto 41',
    '	schema_definition	goto 42',
    '	SQL_schema_manipulation_statement	goto 43',
    '	SQL_schema_definition_statement	goto 44',
    '	SQL_session_statement	goto 45',
    '	SQL_connection_statement	goto 46',
    '	SQL_transaction_statement	goto 47',
    '	SQL_schema_statement	goto 48',
    '	explicit_table	goto 49',
    '	table_value_constructor	goto 50',
    '	query_specification	goto 51',
    '	table_subquery	goto 52',
    '	simple_table	goto 53',
    '	non_join_query_primary	goto 54',
    '	query_term	goto 55',
    '	non_join_query_term	goto 56',
    '	query_expression	goto 57',
    '	temporary_table_declaration	goto 58',
    '	actual_identifier	goto 61',
    '	identifier	goto 62',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 3:',
    '',
    '	sql_script : sql_statement _	(900)',
    '',
    '	.	reduce 900',
    '',
    'state 4:',
    '',
    '	direct_SQL_data_statement : direct_select_statement__multiple_rows _	(893)',
    '',
    '	.	reduce 893',
    '',
    'state 5:',
    '',
    '	direct_SQL_statement : direct_implementation_defined_statement _	(891)',
    '',
    '	.	reduce 891',
    '',
    'state 6:',
    '',
    '	direct_SQL_statement : direct_SQL_data_statement _	(886)',
    '',
    '	.	reduce 886',
    '',
    'state 7:',
    '',
    '	sql_statement : direct_SQL_statement _	(899)',
    '',
    '	.	reduce 899',
    '',
    'state 8:',
    '',
    '	SQL_session_statement : set_local_time_zone_statement _	(876)',
    '',
    '	.	reduce 876',
    '',
    'state 9:',
    '',
    '	SQL_session_statement : set_session_authorization_identifier_statement _	(875)',
    '',
    '	.	reduce 875',
    '',
    'state 10:',
    '',
    '	SQL_session_statement : set_names_statement _	(874)',
    '',
    '	.	reduce 874',
    '',
    'state 11:',
    '',
    '	SQL_session_statement : set_schema_statement _	(873)',
    '',
    '	.	reduce 873',
    '',
    'state 12:',
    '',
    '	SQL_session_statement : set_catalog_statement _	(872)',
    '',
    '	.	reduce 872',
    '',
    'state 13:',
    '',
    '	SQL_connection_statement : disconnect_statement _	(854)',
    '',
    '	.	reduce 854',
    '',
    'state 14:',
    '',
    '	SQL_connection_statement : set_connection_statement _	(853)',
    '',
    '	.	reduce 853',
    '',
    'state 15:',
    '',
    '	SQL_connection_statement : connect_statement _	(852)',
    '',
    '	.	reduce 852',
    '',
    'state 16:',
    '',
    '	SQL_transaction_statement : rollback_statement _	(825)',
    '',
    '	.	reduce 825',
    '',
    'state 17:',
    '',
    '	SQL_transaction_statement : commit_statement _	(824)',
    '',
    '	.	reduce 824',
    '',
    'state 18:',
    '',
    '	SQL_transaction_statement : set_constraints_mode_statement _	(823)',
    '',
    '	.	reduce 823',
    '',
    'state 19:',
    '',
    '	SQL_transaction_statement : set_transaction_statement _	(822)',
    '',
    '	.	reduce 822',
    '',
    'state 20:',
    '',
    '	direct_SQL_data_statement : update_statement__searched _	(895)',
    '',
    '	.	reduce 895',
    '',
    'state 21:',
    '',
    '	direct_SQL_data_statement : insert_statement _	(894)',
    '',
    '	.	reduce 894',
    '',
    'state 22:',
    '',
    '	direct_SQL_data_statement : delete_statement__searched _	(892)',
    '',
    '	.	reduce 892',
    '',
    'state 23:',
    '',
    '	SQL_schema_manipulation_statement : drop_assertion_statement _	(738)',
    '',
    '	.	reduce 738',
    '',
    'state 24:',
    '',
    '	SQL_schema_manipulation_statement : drop_translation_statement _	(737)',
    '',
    '	.	reduce 737',
    '',
    'state 25:',
    '',
    '	SQL_schema_manipulation_statement : drop_collation_statement _	(736)',
    '',
    '	.	reduce 736',
    '',
    'state 26:',
    '',
    '	SQL_schema_manipulation_statement : drop_character_set_statement _	(735)',
    '',
    '	.	reduce 735',
    '',
    'state 27:',
    '',
    '	SQL_schema_manipulation_statement : drop_domain_statement _	(734)',
    '',
    '	.	reduce 734',
    '',
    'state 28:',
    '',
    '	SQL_schema_manipulation_statement : alter_domain_statement _	(733)',
    '',
    '	.	reduce 733',
    '',
    'state 29:',
    '',
    '	SQL_schema_manipulation_statement : revoke_statement _	(732)',
    '',
    '	.	reduce 732',
    '',
    'state 30:',
    '',
    '	SQL_schema_manipulation_statement : drop_view_statement _	(731)',
    '',
    '	.	reduce 731',
    '',
    'state 31:',
    '',
    '	SQL_schema_manipulation_statement : drop_table_statement _	(730)',
    '',
    '	.	reduce 730',
    '',
    'state 32:',
    '',
    '	SQL_schema_manipulation_statement : alter_table_statement _	(729)',
    '',
    '	.	reduce 729',
    '',
    'state 33:',
    '',
    '	SQL_schema_manipulation_statement : drop_schema_statement _	(728)',
    '',
    '	.	reduce 728',
    '',
    'state 34:',
    '',
    '	SQL_schema_definition_statement : assertion_definition _	(628)',
    '',
    '	.	reduce 628',
    '',
    'state 35:',
    '',
    '	SQL_schema_definition_statement : translation_definition _	(627)',
    '',
    '	.	reduce 627',
    '',
    'state 36:',
    '',
    '	SQL_schema_definition_statement : collation_definition _	(626)',
    '',
    '	.	reduce 626',
    '',
    'state 37:',
    '',
    '	SQL_schema_definition_statement : character_set_definition _	(625)',
    '',
    '	.	reduce 625',
    '',
    'state 38:',
    '',
    '	SQL_schema_definition_statement : domain_definition _	(624)',
    '',
    '	.	reduce 624',
    '',
    'state 39:',
    '',
    '	SQL_schema_definition_statement : grant_statement _	(623)',
    '',
    '	.	reduce 623',
    '',
    'state 40:',
    '',
    '	SQL_schema_definition_statement : view_definition _	(622)',
    '',
    '	.	reduce 622',
    '',
    'state 41:',
    '',
    '	SQL_schema_definition_statement : table_definition _	(621)',
    '',
    '	.	reduce 621',
    '',
    'state 42:',
    '',
    '	SQL_schema_definition_statement : schema_definition _	(620)',
    '',
    '	.	reduce 620',
    '',
    'state 43:',
    '',
    '	SQL_schema_statement : SQL_schema_manipulation_statement _	(619)',
    '',
    '	.	reduce 619',
    '',
    'state 44:',
    '',
    '	SQL_schema_statement : SQL_schema_definition_statement _	(618)',
    '',
    '	.	reduce 618',
    '',
    'state 45:',
    '',
    '	direct_SQL_statement : SQL_session_statement _	(890)',
    '',
    '	.	reduce 890',
    '',
    'state 46:',
    '',
    '	direct_SQL_statement : SQL_connection_statement _	(889)',
    '',
    '	.	reduce 889',
    '',
    'state 47:',
    '',
    '	direct_SQL_statement : SQL_transaction_statement _	(888)',
    '',
    '	.	reduce 888',
    '',
    'state 48:',
    '',
    '	direct_SQL_statement : SQL_schema_statement _	(887)',
    '',
    '	.	reduce 887',
    '',
    'state 49:',
    '',
    '	simple_table : explicit_table _	(364)',
    '',
    '	.	reduce 364',
    '',
    'state 50:',
    '',
    '	simple_table : table_value_constructor _	(363)',
    '',
    '	.	reduce 363',
    '',
    'state 51:',
    '',
    '	simple_table : query_specification _	(362)',
    '',
    '	.	reduce 362',
    '',
    'state 52:',
    '',
    '	non_join_query_primary : table_subquery _	(361)',
    '',
    '	.	reduce 361',
    '',
    'state 53:',
    '',
    '	non_join_query_primary : simple_table _	(360)',
    '',
    '	.	reduce 360',
    '',
    'state 54:',
    '',
    '	non_join_query_term : non_join_query_primary _	(354)',
    '',
    '	.	reduce 354',
    '',
    'state 55:',
    '',
    '	non_join_query_term : query_term _ _INTERSECT all_opt corresponding_spec_opt query_primary',
    '',
    '	_INTERSECT	shift 89',
    '	.	error',
    '',
    'state 56:',
    '',
    '	query_expression : non_join_query_term _	(351)',
    '	query_term : non_join_query_term _	(435)',
    '',
    '	$end	reduce 351',
    '	identifier_body	reduce 351',
    '	delimited_identifier	reduce 351',
    '	left_paren	reduce 351',
    '	right_paren	reduce 351',
    '	semicolon	reduce 351',
    '	underscore	reduce 351',
    '	_ALTER	reduce 351',
    '	_COMMIT	reduce 351',
    '	_CONNECT	reduce 351',
    '	_CREATE	reduce 351',
    '	_DECLARE	reduce 351',
    '	_DELETE	reduce 351',
    '	_DISCONNECT	reduce 351',
    '	_DROP	reduce 351',
    '	_EXCEPT	reduce 351',
    '	_FOR	reduce 351',
    '	_GRANT	reduce 351',
    '	_INSERT	reduce 351',
    '	_ORDER	reduce 351',
    '	_REVOKE	reduce 351',
    '	_ROLLBACK	reduce 351',
    '	_SELECT	reduce 351',
    '	_SET	reduce 351',
    '	_TABLE	reduce 351',
    '	_UNION	reduce 351',
    '	_UPDATE	reduce 351',
    '	_VALUES	reduce 351',
    '	_WITH	reduce 351',
    '	_INTERSECT	reduce 435',
    '	.	error',
    '',
    'state 57:',
    '',
    '	query_expression : query_expression _ _UNION all_opt corresponding_spec_opt query_term',
    '	query_expression : query_expression _ _EXCEPT all_opt corresponding_spec_opt query_term',
    '	direct_select_statement__multiple_rows : query_expression _ order_by_clause_opt',
    '	order_by_clause_opt : _	(587)',
    '',
    '	_EXCEPT	shift 91',
    '	_ORDER	shift 92',
    '	_UNION	shift 93',
    '	$end	reduce 587',
    '	identifier_body	reduce 587',
    '	delimited_identifier	reduce 587',
    '	left_paren	reduce 587',
    '	underscore	reduce 587',
    '	_ALTER	reduce 587',
    '	_COMMIT	reduce 587',
    '	_CONNECT	reduce 587',
    '	_CREATE	reduce 587',
    '	_DECLARE	reduce 587',
    '	_DELETE	reduce 587',
    '	_DISCONNECT	reduce 587',
    '	_DROP	reduce 587',
    '	_GRANT	reduce 587',
    '	_INSERT	reduce 587',
    '	_REVOKE	reduce 587',
    '	_ROLLBACK	reduce 587',
    '	_SELECT	reduce 587',
    '	_SET	reduce 587',
    '	_TABLE	reduce 587',
    '	_UPDATE	reduce 587',
    '	_VALUES	reduce 587',
    '	.	error',
    '',
    '	order_by_clause_opt	goto 90',
    '',
    'state 58:',
    '',
    '	direct_SQL_data_statement : temporary_table_declaration _	(896)',
    '',
    '	.	reduce 896',
    '',
    'state 59:',
    '',
    '	module : module_name_clause _ language_clause module_authorization_clause module_opt',
    '',
    '	_LANGUAGE	shift 95',
    '	.	error',
    '',
    '	language_clause	goto 94',
    '',
    'state 60:',
    '',
    '	sql_input : module _	(903)',
    '',
    '	.	reduce 903',
    '',
    'state 61:',
    '',
    '	identifier : actual_identifier _	(39)',
    '',
    '	.	reduce 39',
    '',
    'state 62:',
    '',
    '	direct_implementation_defined_statement : identifier _	(898)',
    '',
    '	.	reduce 898',
    '',
    'state 63:',
    '',
    '	identifier : introducer _ character_set_specification actual_identifier',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	actual_identifier	goto 61',
    '	SQL_language_identifier	goto 96',
    '	identifier	goto 97',
    '	character_set_name	goto 98',
    '	character_set_specification	goto 99',
    '	introducer	goto 63',
    '	regular_identifier	goto 100',
    '',
    'state 64:',
    '',
    '	actual_identifier : regular_identifier _	(40)',
    '',
    '	.	reduce 40',
    '',
    'state 65:',
    '',
    '	$accept : SQL92Grammar _ $end',
    '',
    '	$end	accept',
    '	.	error',
    '',
    'state 66:',
    '',
    '	regular_identifier : identifier_body _	(1)',
    '',
    '	.	reduce 1',
    '',
    'state 67:',
    '',
    '	actual_identifier : delimited_identifier _	(41)',
    '',
    '	.	reduce 41',
    '',
    'state 68:',
    '',
    '	table_subquery : left_paren _ query_expression right_paren',
    '',
    '	left_paren	shift 68',
    '	_SELECT	shift 83',
    '	_TABLE	shift 85',
    '	_VALUES	shift 87',
    '	.	error',
    '',
    '	explicit_table	goto 49',
    '	table_value_constructor	goto 50',
    '	query_specification	goto 51',
    '	table_subquery	goto 52',
    '	simple_table	goto 53',
    '	non_join_query_primary	goto 54',
    '	query_term	goto 55',
    '	non_join_query_term	goto 56',
    '	query_expression	goto 101',
    '',
    'state 69:',
    '',
    '	introducer : underscore _	(31)',
    '',
    '	.	reduce 31',
    '',
    'state 70:',
    '',
    '	alter_table_statement : _ALTER _ _TABLE table_name alter_table_action',
    '	alter_domain_statement : _ALTER _ _DOMAIN domain_name alter_domain_action',
    '',
    '	_DOMAIN	shift 102',
    '	_TABLE	shift 103',
    '	.	error',
    '',
    'state 71:',
    '',
    '	commit_statement : _COMMIT _	(848)',
    '	commit_statement : _COMMIT _ _WORK',
    '',
    '	_WORK	shift 104',
    '	$end	reduce 848',
    '	identifier_body	reduce 848',
    '	delimited_identifier	reduce 848',
    '	left_paren	reduce 848',
    '	semicolon	reduce 848',
    '	underscore	reduce 848',
    '	_ALTER	reduce 848',
    '	_COMMIT	reduce 848',
    '	_CONNECT	reduce 848',
    '	_CREATE	reduce 848',
    '	_DECLARE	reduce 848',
    '	_DELETE	reduce 848',
    '	_DISCONNECT	reduce 848',
    '	_DROP	reduce 848',
    '	_GRANT	reduce 848',
    '	_INSERT	reduce 848',
    '	_REVOKE	reduce 848',
    '	_ROLLBACK	reduce 848',
    '	_SELECT	reduce 848',
    '	_SET	reduce 848',
    '	_TABLE	reduce 848',
    '	_UPDATE	reduce 848',
    '	_VALUES	reduce 848',
    '	.	error',
    '',
    'state 72:',
    '',
    '	connect_statement : _CONNECT _ _TO connection_target',
    '',
    '	_TO	shift 105',
    '	.	error',
    '',
    'state 73:',
    '',
    '	schema_definition : _CREATE _ _SCHEMA schema_name_clause schema_character_set_specification_opt schema_elements',
    '	domain_definition : _CREATE _ _DOMAIN domain_name as_opt data_type default_clause_opt domain_constraint_opt collate_clause_opt',
    '	table_definition : _CREATE _ table_definition_opts _TABLE table_name table_element_list table_commit_opts',
    '	view_definition : _CREATE _ _VIEW table_name view_column_list_opt _AS query_expression view_check_opt',
    '	assertion_definition : _CREATE _ _ASSERTION constraint_name assertion_check constraint_attributes_opt',
    '	character_set_definition : _CREATE _ _CHARACTER _SET character_set_name as_opt character_set_source charset_collation_opt',
    '	collation_definition : _CREATE _ _COLLATION collation_name _FOR character_set_specification _FROM collation_source pad_attribute_opt',
    '	translation_definition : _CREATE _ _TRANSLATION translation_name _FOR source_character_set_specification _TO target_character_set_specification _FROM translation_source',
    '	table_definition_opts : _	(652)',
    '',
    '	_ASSERTION	shift 107',
    '	_CHARACTER	shift 108',
    '	_COLLATION	shift 109',
    '	_DOMAIN	shift 110',
    '	_GLOBAL	shift 111',
    '	_LOCAL	shift 112',
    '	_SCHEMA	shift 113',
    '	_TRANSLATION	shift 114',
    '	_VIEW	shift 115',
    '	_TABLE	reduce 652',
    '	.	error',
    '',
    '	table_definition_opts	goto 106',
    '',
    'state 74:',
    '',
    '	temporary_table_declaration : _DECLARE _ _LOCAL _TEMPORARY _TABLE qualified_local_table_name table_element_list temporary_table_declaration_opt',
    '',
    '	_LOCAL	shift 116',
    '	.	error',
    '',
    'state 75:',
    '',
    '	delete_statement__searched : _DELETE _ _FROM table_name where_clause_opt',
    '',
    '	_FROM	shift 117',
    '	.	error',
    '',
    'state 76:',
    '',
    '	disconnect_statement : _DISCONNECT _ disconnect_object',
    '',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	digit	shift 147',
    '	plus_sign	shift 148',
    '	minus_sign	shift 149',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_ALL	shift 152',
    '	_CURRENT	shift 153',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 155',
    '	_INTERVAL	shift 156',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	.	error',
    '',
    '	disconnect_object	goto 118',
    '	connection_object	goto 119',
    '	connection_name	goto 120',
    '	simple_value_specification	goto 121',
    '	parameter_name	goto 122',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 128',
    '	signed_numeric_literal	goto 129',
    '	literal	goto 130',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 132',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	sign	goto 137',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 142',
    '',
    'state 77:',
    '',
    '	drop_schema_statement : _DROP _ _SCHEMA schema_name drop_behaviour',
    '	drop_table_statement : _DROP _ _TABLE table_name drop_behaviour',
    '	drop_view_statement : _DROP _ _VIEW table_name drop_behaviour',
    '	drop_domain_statement : _DROP _ _DOMAIN domain_name drop_behaviour',
    '	drop_character_set_statement : _DROP _ _CHARACTER _SET character_set_name',
    '	drop_collation_statement : _DROP _ _COLLATION collation_name',
    '	drop_translation_statement : _DROP _ _TRANSLATION translation_name',
    '	drop_assertion_statement : _DROP _ _ASSERTION constraint_name',
    '',
    '	_ASSERTION	shift 159',
    '	_CHARACTER	shift 160',
    '	_COLLATION	shift 161',
    '	_DOMAIN	shift 162',
    '	_SCHEMA	shift 163',
    '	_TABLE	shift 164',
    '	_TRANSLATION	shift 165',
    '	_VIEW	shift 166',
    '	.	error',
    '',
    'state 78:',
    '',
    '	grant_statement : _GRANT _ privileges _ON object_name _TO grantee_list grant_option',
    '',
    '	_ALL	shift 170',
    '	_DELETE	shift 171',
    '	_INSERT	shift 172',
    '	_REFERENCES	shift 173',
    '	_SELECT	shift 174',
    '	_UPDATE	shift 175',
    '	_USAGE	shift 176',
    '	.	error',
    '',
    '	action	goto 167',
    '	action_list	goto 168',
    '	privileges	goto 169',
    '',
    'state 79:',
    '',
    '	insert_statement : _INSERT _ _INTO table_name insert_columns_and_source',
    '',
    '	_INTO	shift 177',
    '	.	error',
    '',
    'state 80:',
    '',
    '	module_name_clause : _MODULE _ _MODULE module_name _MODULE module_character_set_specification _MODULE module_name module_character_set_specification',
    '',
    '	_MODULE	shift 178',
    '	.	error',
    '',
    'state 81:',
    '',
    '	revoke_statement : _REVOKE _ grant_option_for_opt privileges _ON object_name _FROM grantee_list drop_behaviour',
    '	grant_option_for_opt : _	(762)',
    '',
    '	_GRANT	shift 180',
    '	_ALL	reduce 762',
    '	_DELETE	reduce 762',
    '	_INSERT	reduce 762',
    '	_REFERENCES	reduce 762',
    '	_SELECT	reduce 762',
    '	_UPDATE	reduce 762',
    '	_USAGE	reduce 762',
    '	.	error',
    '',
    '	grant_option_for_opt	goto 179',
    '',
    'state 82:',
    '',
    '	rollback_statement : _ROLLBACK _	(850)',
    '	rollback_statement : _ROLLBACK _ _WORK',
    '',
    '	_WORK	shift 181',
    '	$end	reduce 850',
    '	identifier_body	reduce 850',
    '	delimited_identifier	reduce 850',
    '	left_paren	reduce 850',
    '	semicolon	reduce 850',
    '	underscore	reduce 850',
    '	_ALTER	reduce 850',
    '	_COMMIT	reduce 850',
    '	_CONNECT	reduce 850',
    '	_CREATE	reduce 850',
    '	_DECLARE	reduce 850',
    '	_DELETE	reduce 850',
    '	_DISCONNECT	reduce 850',
    '	_DROP	reduce 850',
    '	_GRANT	reduce 850',
    '	_INSERT	reduce 850',
    '	_REVOKE	reduce 850',
    '	_ROLLBACK	reduce 850',
    '	_SELECT	reduce 850',
    '	_SET	reduce 850',
    '	_TABLE	reduce 850',
    '	_UPDATE	reduce 850',
    '	_VALUES	reduce 850',
    '	.	error',
    '',
    'state 83:',
    '',
    '	query_specification : _SELECT _ set_quantifier_opt select_list table_expression',
    '	set_quantifier_opt : _	(349)',
    '',
    '	_ALL	shift 184',
    '	_DISTINCT	shift 185',
    '	identifier_body	reduce 349',
    '	national_character_string_literal_start	reduce 349',
    '	bit_string_literal_start	reduce 349',
    '	string_literal_continuation	reduce 349',
    '	hex_string_literal_start	reduce 349',
    '	delimited_identifier	reduce 349',
    '	digit	reduce 349',
    '	left_paren	reduce 349',
    '	asterisk	reduce 349',
    '	plus_sign	reduce 349',
    '	minus_sign	reduce 349',
    '	period	reduce 349',
    '	colon	reduce 349',
    '	underscore	reduce 349',
    '	_AVG	reduce 349',
    '	_BIT_LENGTH	reduce 349',
    '	_CASE	reduce 349',
    '	_CAST	reduce 349',
    '	_CHARACTER_LENGTH	reduce 349',
    '	_CHAR_LENGTH	reduce 349',
    '	_COALESCE	reduce 349',
    '	_CONVERT	reduce 349',
    '	_CURRENT_DATE	reduce 349',
    '	_CURRENT_TIME	reduce 349',
    '	_CURRENT_TIMESTAMP	reduce 349',
    '	_CURRENT_USER	reduce 349',
    '	_DATE	reduce 349',
    '	_DEFAULT	reduce 349',
    '	_EXTRACT	reduce 349',
    '	_INTERVAL	reduce 349',
    '	_LOWER	reduce 349',
    '	_MAX	reduce 349',
    '	_MIN	reduce 349',
    '	_NULL	reduce 349',
    '	_NULLIF	reduce 349',
    '	_OCTET_LENGTH	reduce 349',
    '	_POSITION	reduce 349',
    '	_SESSION_USER	reduce 349',
    '	_SUBSTRING	reduce 349',
    '	_SUM	reduce 349',
    '	_SYSTEM_USER	reduce 349',
    '	_TIME	reduce 349',
    '	_TIMESTAMP	reduce 349',
    '	_TRANSLATE	reduce 349',
    '	_TRIM	reduce 349',
    '	_UPPER	reduce 349',
    '	_USER	reduce 349',
    '	_VALUE	reduce 349',
    '	_COUNT	reduce 349',
    '	.	error',
    '',
    '	set_quantifier_opt	goto 182',
    '	set_quantifier	goto 183',
    '',
    'state 84:',
    '',
    '	set_transaction_statement : _SET _ _TRANSACTION transaction_mode_list',
    '	set_constraints_mode_statement : _SET _ _CONSTRAINTS constraint_name_list _DEFERRED',
    '	set_constraints_mode_statement : _SET _ _CONSTRAINTS constraint_name_list _IMMEDIATE',
    '	set_connection_statement : _SET _ _CONNECTION connection_object',
    '	set_catalog_statement : _SET _ _CATALOG value_specification',
    '	set_schema_statement : _SET _ _SCHEMA value_specification',
    '	set_names_statement : _SET _ _NAMES value_specification',
    '	set_session_authorization_identifier_statement : _SET _ _SESSION _AUTHORIZATION value_specification',
    '	set_local_time_zone_statement : _SET _ _TIME _ZONE set_time_zone_value',
    '',
    '	_CATALOG	shift 186',
    '	_CONNECTION	shift 187',
    '	_CONSTRAINTS	shift 188',
    '	_NAMES	shift 189',
    '	_SCHEMA	shift 190',
    '	_SESSION	shift 191',
    '	_TIME	shift 192',
    '	_TRANSACTION	shift 193',
    '	.	error',
    '',
    'state 85:',
    '',
    '	explicit_table : _TABLE _ table_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	table_name	goto 194',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 86:',
    '',
    '	update_statement__searched : _UPDATE _ table_name _SET set_clause_list where_clause_opt',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	table_name	goto 199',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 87:',
    '',
    '	table_value_constructor : _VALUES _ table_value_constructor_list',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 248',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	table_value_constructor_list	goto 216',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 238',
    '	row_value_constructor	goto 239',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 88:',
    '',
    '	sql_script : sql_script sql_statement _	(901)',
    '',
    '	.	reduce 901',
    '',
    'state 89:',
    '',
    '	non_join_query_term : query_term _INTERSECT _ all_opt corresponding_spec_opt query_primary',
    '	all_opt : _	(356)',
    '',
    '	_ALL	shift 283',
    '	left_paren	reduce 356',
    '	_CORRESPONDING	reduce 356',
    '	_SELECT	reduce 356',
    '	_TABLE	reduce 356',
    '	_VALUES	reduce 356',
    '	.	error',
    '',
    '	all_opt	goto 282',
    '',
    'state 90:',
    '',
    '	direct_select_statement__multiple_rows : query_expression order_by_clause_opt _	(897)',
    '',
    '	.	reduce 897',
    '',
    'state 91:',
    '',
    '	query_expression : query_expression _EXCEPT _ all_opt corresponding_spec_opt query_term',
    '	all_opt : _	(356)',
    '',
    '	_ALL	shift 283',
    '	left_paren	reduce 356',
    '	_CORRESPONDING	reduce 356',
    '	_SELECT	reduce 356',
    '	_TABLE	reduce 356',
    '	_VALUES	reduce 356',
    '	.	error',
    '',
    '	all_opt	goto 284',
    '',
    'state 92:',
    '',
    '	order_by_clause_opt : _ORDER _ _BY sort_specification_list',
    '',
    '	_BY	shift 285',
    '	.	error',
    '',
    'state 93:',
    '',
    '	query_expression : query_expression _UNION _ all_opt corresponding_spec_opt query_term',
    '	all_opt : _	(356)',
    '',
    '	_ALL	shift 283',
    '	left_paren	reduce 356',
    '	_CORRESPONDING	reduce 356',
    '	_SELECT	reduce 356',
    '	_TABLE	reduce 356',
    '	_VALUES	reduce 356',
    '	.	error',
    '',
    '	all_opt	goto 286',
    '',
    'state 94:',
    '',
    '	module : module_name_clause language_clause _ module_authorization_clause module_opt',
    '',
    '	_AUTHORIZATION	shift 288',
    '	_SCHEMA	shift 289',
    '	.	error',
    '',
    '	module_authorization_clause	goto 287',
    '',
    'state 95:',
    '',
    '	language_clause : _LANGUAGE _ language_name',
    '',
    '	_ADA	shift 291',
    '	_C	shift 292',
    '	_COBOL	shift 293',
    '	_FORTRAN	shift 294',
    '	_MUMPS	shift 295',
    '	_PASCAL	shift 296',
    '	_PLI	shift 297',
    '	.	error',
    '',
    '	language_name	goto 290',
    '',
    'state 96:',
    '',
    '	character_set_name : SQL_language_identifier _	(35)',
    '',
    '	.	reduce 35',
    '',
    'state 97:',
    '',
    '	character_set_name : identifier _ period identifier period SQL_language_identifier',
    '	character_set_name : identifier _ period SQL_language_identifier',
    '',
    '	period	shift 298',
    '	.	error',
    '',
    'state 98:',
    '',
    '	character_set_specification : character_set_name _	(32)',
    '',
    '	.	reduce 32',
    '',
    'state 99:',
    '',
    '	identifier : introducer character_set_specification _ actual_identifier',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	.	error',
    '',
    '	actual_identifier	goto 299',
    '	regular_identifier	goto 64',
    '',
    'state 100:',
    '',
    '	actual_identifier : regular_identifier _	(40)',
    '	SQL_language_identifier : regular_identifier _	(42)',
    '',
    '	period	reduce 40',
    '	$end	reduce 42',
    '	identifier_body	reduce 42',
    '	string_literal_continuation	reduce 42',
    '	delimited_identifier	reduce 42',
    '	left_paren	reduce 42',
    '	right_paren	reduce 42',
    '	comma	reduce 42',
    '	semicolon	reduce 42',
    '	underscore	reduce 42',
    '	_ALTER	reduce 42',
    '	_AS	reduce 42',
    '	_CHECK	reduce 42',
    '	_COLLATE	reduce 42',
    '	_COLLATION	reduce 42',
    '	_COMMIT	reduce 42',
    '	_CONNECT	reduce 42',
    '	_CONSTRAINT	reduce 42',
    '	_CREATE	reduce 42',
    '	_DECLARE	reduce 42',
    '	_DEFAULT	reduce 42',
    '	_DELETE	reduce 42',
    '	_DISCONNECT	reduce 42',
    '	_DROP	reduce 42',
    '	_FROM	reduce 42',
    '	_GET	reduce 42',
    '	_GRANT	reduce 42',
    '	_INSERT	reduce 42',
    '	_LANGUAGE	reduce 42',
    '	_MODULE	reduce 42',
    '	_NOT	reduce 42',
    '	_PRIMARY	reduce 42',
    '	_REFERENCES	reduce 42',
    '	_REVOKE	reduce 42',
    '	_ROLLBACK	reduce 42',
    '	_SELECT	reduce 42',
    '	_SET	reduce 42',
    '	_TABLE	reduce 42',
    '	_TO	reduce 42',
    '	_UNIQUE	reduce 42',
    '	_UPDATE	reduce 42',
    '	_VALUES	reduce 42',
    '	.	error',
    '',
    'state 101:',
    '',
    '	table_subquery : left_paren query_expression _ right_paren',
    '	query_expression : query_expression _ _UNION all_opt corresponding_spec_opt query_term',
    '	query_expression : query_expression _ _EXCEPT all_opt corresponding_spec_opt query_term',
    '',
    '	right_paren	shift 300',
    '	_EXCEPT	shift 91',
    '	_UNION	shift 93',
    '	.	error',
    '',
    'state 102:',
    '',
    '	alter_domain_statement : _ALTER _DOMAIN _ domain_name alter_domain_action',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	qualified_name	goto 301',
    '	domain_name	goto 302',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 103:',
    '',
    '	alter_table_statement : _ALTER _TABLE _ table_name alter_table_action',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	table_name	goto 303',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 104:',
    '',
    '	commit_statement : _COMMIT _WORK _	(849)',
    '',
    '	.	reduce 849',
    '',
    'state 105:',
    '',
    '	connect_statement : _CONNECT _TO _ connection_target',
    '',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	digit	shift 147',
    '	plus_sign	shift 148',
    '	minus_sign	shift 149',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 307',
    '	_INTERVAL	shift 156',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	.	error',
    '',
    '	SQL_server_name	goto 304',
    '	connection_target	goto 305',
    '	simple_value_specification	goto 306',
    '	parameter_name	goto 122',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 128',
    '	signed_numeric_literal	goto 129',
    '	literal	goto 130',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 132',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	sign	goto 137',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 142',
    '',
    'state 106:',
    '',
    '	table_definition : _CREATE table_definition_opts _ _TABLE table_name table_element_list table_commit_opts',
    '',
    '	_TABLE	shift 308',
    '	.	error',
    '',
    'state 107:',
    '',
    '	assertion_definition : _CREATE _ASSERTION _ constraint_name assertion_check constraint_attributes_opt',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	constraint_name	goto 309',
    '	qualified_name	goto 310',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 108:',
    '',
    '	character_set_definition : _CREATE _CHARACTER _ _SET character_set_name as_opt character_set_source charset_collation_opt',
    '',
    '	_SET	shift 311',
    '	.	error',
    '',
    'state 109:',
    '',
    '	collation_definition : _CREATE _COLLATION _ collation_name _FOR character_set_specification _FROM collation_source pad_attribute_opt',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	collation_name	goto 312',
    '	qualified_name	goto 313',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 110:',
    '',
    '	domain_definition : _CREATE _DOMAIN _ domain_name as_opt data_type default_clause_opt domain_constraint_opt collate_clause_opt',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	qualified_name	goto 301',
    '	domain_name	goto 314',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 111:',
    '',
    '	table_definition_opts : _GLOBAL _ _TEMPORARY',
    '',
    '	_TEMPORARY	shift 315',
    '	.	error',
    '',
    'state 112:',
    '',
    '	table_definition_opts : _LOCAL _ _TEMPORARY',
    '',
    '	_TEMPORARY	shift 316',
    '	.	error',
    '',
    'state 113:',
    '',
    '	schema_definition : _CREATE _SCHEMA _ schema_name_clause schema_character_set_specification_opt schema_elements',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_AUTHORIZATION	shift 320',
    '	.	error',
    '',
    '	schema_name_clause	goto 317',
    '	actual_identifier	goto 61',
    '	schema_name	goto 318',
    '	identifier	goto 319',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 114:',
    '',
    '	translation_definition : _CREATE _TRANSLATION _ translation_name _FOR source_character_set_specification _TO target_character_set_specification _FROM translation_source',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	translation_name	goto 321',
    '	qualified_name	goto 322',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 115:',
    '',
    '	view_definition : _CREATE _VIEW _ table_name view_column_list_opt _AS query_expression view_check_opt',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	table_name	goto 323',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 116:',
    '',
    '	temporary_table_declaration : _DECLARE _LOCAL _ _TEMPORARY _TABLE qualified_local_table_name table_element_list temporary_table_declaration_opt',
    '',
    '	_TEMPORARY	shift 324',
    '	.	error',
    '',
    'state 117:',
    '',
    '	delete_statement__searched : _DELETE _FROM _ table_name where_clause_opt',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	table_name	goto 325',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 118:',
    '',
    '	disconnect_statement : _DISCONNECT disconnect_object _	(868)',
    '',
    '	.	reduce 868',
    '',
    'state 119:',
    '',
    '	disconnect_object : connection_object _	(869)',
    '',
    '	.	reduce 869',
    '',
    'state 120:',
    '',
    '	connection_object : connection_name _	(867)',
    '',
    '	.	reduce 867',
    '',
    'state 121:',
    '',
    '	connection_name : simple_value_specification _	(863)',
    '',
    '	.	reduce 863',
    '',
    'state 122:',
    '',
    '	simple_value_specification : parameter_name _	(794)',
    '',
    '	.	reduce 794',
    '',
    'state 123:',
    '',
    '	datetime_literal : timestamp_literal _	(213)',
    '',
    '	.	reduce 213',
    '',
    'state 124:',
    '',
    '	datetime_literal : time_literal _	(212)',
    '',
    '	.	reduce 212',
    '',
    'state 125:',
    '',
    '	datetime_literal : date_literal _	(211)',
    '',
    '	.	reduce 211',
    '',
    'state 126:',
    '',
    '	general_literal : interval_literal _	(210)',
    '',
    '	.	reduce 210',
    '',
    'state 127:',
    '',
    '	general_literal : datetime_literal _	(209)',
    '',
    '	.	reduce 209',
    '',
    'state 128:',
    '',
    '	literal : general_literal _	(202)',
    '',
    '	.	reduce 202',
    '',
    'state 129:',
    '',
    '	literal : signed_numeric_literal _	(201)',
    '',
    '	.	reduce 201',
    '',
    'state 130:',
    '',
    '	simple_value_specification : literal _	(795)',
    '',
    '	.	reduce 795',
    '',
    'state 131:',
    '',
    '	character_string_literal : character_string_literal_main _	(28)',
    '	character_string_literal_main : character_string_literal_main _ string_literal_continuation',
    '',
    '	string_literal_continuation	shift 326',
    '	$end	reduce 28',
    '	identifier_body	reduce 28',
    '	delimited_identifier	reduce 28',
    '	not_equals_operator	reduce 28',
    '	greater_than_or_equals_operator	reduce 28',
    '	less_than_or_equals_operator	reduce 28',
    '	concatenation_operator	reduce 28',
    '	left_paren	reduce 28',
    '	right_paren	reduce 28',
    '	asterisk	reduce 28',
    '	plus_sign	reduce 28',
    '	comma	reduce 28',
    '	minus_sign	reduce 28',
    '	solidus	reduce 28',
    '	semicolon	reduce 28',
    '	less_than_operator	reduce 28',
    '	equals_operator	reduce 28',
    '	greater_than_operator	reduce 28',
    '	underscore	reduce 28',
    '	_ALTER	reduce 28',
    '	_AND	reduce 28',
    '	_AS	reduce 28',
    '	_AT	reduce 28',
    '	_BETWEEN	reduce 28',
    '	_CHECK	reduce 28',
    '	_COLLATE	reduce 28',
    '	_COMMIT	reduce 28',
    '	_CONNECT	reduce 28',
    '	_CONSTRAINT	reduce 28',
    '	_CREATE	reduce 28',
    '	_CROSS	reduce 28',
    '	_DAY	reduce 28',
    '	_DECLARE	reduce 28',
    '	_DELETE	reduce 28',
    '	_DISCONNECT	reduce 28',
    '	_DROP	reduce 28',
    '	_ELSE	reduce 28',
    '	_END	reduce 28',
    '	_ESCAPE	reduce 28',
    '	_EXCEPT	reduce 28',
    '	_FOR	reduce 28',
    '	_FROM	reduce 28',
    '	_FULL	reduce 28',
    '	_GRANT	reduce 28',
    '	_GROUP	reduce 28',
    '	_HAVING	reduce 28',
    '	_HOUR	reduce 28',
    '	_IN	reduce 28',
    '	_INNER	reduce 28',
    '	_INSERT	reduce 28',
    '	_INTERSECT	reduce 28',
    '	_INTO	reduce 28',
    '	_IS	reduce 28',
    '	_JOIN	reduce 28',
    '	_LEFT	reduce 28',
    '	_LIKE	reduce 28',
    '	_MATCH	reduce 28',
    '	_MINUTE	reduce 28',
    '	_MONTH	reduce 28',
    '	_NATURAL	reduce 28',
    '	_NOT	reduce 28',
    '	_OR	reduce 28',
    '	_ORDER	reduce 28',
    '	_OVERLAPS	reduce 28',
    '	_PRIMARY	reduce 28',
    '	_REFERENCES	reduce 28',
    '	_REVOKE	reduce 28',
    '	_RIGHT	reduce 28',
    '	_ROLLBACK	reduce 28',
    '	_SECOND	reduce 28',
    '	_SELECT	reduce 28',
    '	_SET	reduce 28',
    '	_TABLE	reduce 28',
    '	_THEN	reduce 28',
    '	_UNION	reduce 28',
    '	_UNIQUE	reduce 28',
    '	_UPDATE	reduce 28',
    '	_USER	reduce 28',
    '	_USING	reduce 28',
    '	_VALUES	reduce 28',
    '	_WHEN	reduce 28',
    '	_WHERE	reduce 28',
    '	_WITH	reduce 28',
    '	_YEAR	reduce 28',
    '	.	error',
    '',
    'state 132:',
    '',
    '	character_string_literal : introducer _ character_set_specification character_string_literal_main',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	actual_identifier	goto 61',
    '	SQL_language_identifier	goto 96',
    '	identifier	goto 97',
    '	character_set_name	goto 98',
    '	character_set_specification	goto 327',
    '	introducer	goto 63',
    '	regular_identifier	goto 100',
    '',
    'state 133:',
    '',
    '	general_literal : character_string_literal _	(205)',
    '',
    '	.	reduce 205',
    '',
    'state 134:',
    '',
    '	general_literal : hex_string_literal _	(208)',
    '',
    '	.	reduce 208',
    '',
    'state 135:',
    '',
    '	general_literal : bit_string_literal _	(207)',
    '',
    '	.	reduce 207',
    '',
    'state 136:',
    '',
    '	general_literal : national_character_string_literal _	(206)',
    '',
    '	.	reduce 206',
    '',
    'state 137:',
    '',
    '	signed_numeric_literal : sign _ unsigned_numeric_literal',
    '',
    '	digit	shift 147',
    '	period	shift 150',
    '	.	error',
    '',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 328',
    '',
    'state 138:',
    '',
    '	approximate_numeric_literal : mantissa _ _E exponent',
    '',
    '	_E	shift 329',
    '	.	error',
    '',
    'state 139:',
    '',
    '	exact_numeric_literal : unsigned_integer _ exact_numeric_literal_opt',
    '	unsigned_integer : unsigned_integer _ digit',
    '	exact_numeric_literal_opt : _	(6)',
    '',
    '	digit	shift 331',
    '	period	shift 332',
    '	$end	reduce 6',
    '	identifier_body	reduce 6',
    '	delimited_identifier	reduce 6',
    '	not_equals_operator	reduce 6',
    '	greater_than_or_equals_operator	reduce 6',
    '	less_than_or_equals_operator	reduce 6',
    '	concatenation_operator	reduce 6',
    '	left_paren	reduce 6',
    '	right_paren	reduce 6',
    '	asterisk	reduce 6',
    '	plus_sign	reduce 6',
    '	comma	reduce 6',
    '	minus_sign	reduce 6',
    '	solidus	reduce 6',
    '	semicolon	reduce 6',
    '	less_than_operator	reduce 6',
    '	equals_operator	reduce 6',
    '	greater_than_operator	reduce 6',
    '	underscore	reduce 6',
    '	_ALTER	reduce 6',
    '	_AND	reduce 6',
    '	_AS	reduce 6',
    '	_AT	reduce 6',
    '	_BETWEEN	reduce 6',
    '	_CHECK	reduce 6',
    '	_COLLATE	reduce 6',
    '	_COMMIT	reduce 6',
    '	_CONNECT	reduce 6',
    '	_CONSTRAINT	reduce 6',
    '	_CREATE	reduce 6',
    '	_CROSS	reduce 6',
    '	_DAY	reduce 6',
    '	_DECLARE	reduce 6',
    '	_DELETE	reduce 6',
    '	_DISCONNECT	reduce 6',
    '	_DROP	reduce 6',
    '	_ELSE	reduce 6',
    '	_END	reduce 6',
    '	_ESCAPE	reduce 6',
    '	_EXCEPT	reduce 6',
    '	_FOR	reduce 6',
    '	_FROM	reduce 6',
    '	_FULL	reduce 6',
    '	_GRANT	reduce 6',
    '	_GROUP	reduce 6',
    '	_HAVING	reduce 6',
    '	_HOUR	reduce 6',
    '	_IN	reduce 6',
    '	_INNER	reduce 6',
    '	_INSERT	reduce 6',
    '	_INTERSECT	reduce 6',
    '	_INTO	reduce 6',
    '	_IS	reduce 6',
    '	_JOIN	reduce 6',
    '	_LEFT	reduce 6',
    '	_LIKE	reduce 6',
    '	_MATCH	reduce 6',
    '	_MINUTE	reduce 6',
    '	_MONTH	reduce 6',
    '	_NATURAL	reduce 6',
    '	_NOT	reduce 6',
    '	_OR	reduce 6',
    '	_ORDER	reduce 6',
    '	_OVERLAPS	reduce 6',
    '	_PRIMARY	reduce 6',
    '	_REFERENCES	reduce 6',
    '	_REVOKE	reduce 6',
    '	_RIGHT	reduce 6',
    '	_ROLLBACK	reduce 6',
    '	_SECOND	reduce 6',
    '	_SELECT	reduce 6',
    '	_SET	reduce 6',
    '	_TABLE	reduce 6',
    '	_THEN	reduce 6',
    '	_UNION	reduce 6',
    '	_UNIQUE	reduce 6',
    '	_UPDATE	reduce 6',
    '	_USER	reduce 6',
    '	_USING	reduce 6',
    '	_VALUES	reduce 6',
    '	_WHEN	reduce 6',
    '	_WHERE	reduce 6',
    '	_WITH	reduce 6',
    '	_YEAR	reduce 6',
    '	_E	reduce 6',
    '	.	error',
    '',
    '	exact_numeric_literal_opt	goto 330',
    '',
    'state 140:',
    '',
    '	unsigned_numeric_literal : approximate_numeric_literal _	(3)',
    '',
    '	.	reduce 3',
    '',
    'state 141:',
    '',
    '	unsigned_numeric_literal : exact_numeric_literal _	(2)',
    '	mantissa : exact_numeric_literal _	(12)',
    '',
    '	$end	reduce 2',
    '	identifier_body	reduce 2',
    '	delimited_identifier	reduce 2',
    '	not_equals_operator	reduce 2',
    '	greater_than_or_equals_operator	reduce 2',
    '	less_than_or_equals_operator	reduce 2',
    '	concatenation_operator	reduce 2',
    '	left_paren	reduce 2',
    '	right_paren	reduce 2',
    '	asterisk	reduce 2',
    '	plus_sign	reduce 2',
    '	comma	reduce 2',
    '	minus_sign	reduce 2',
    '	solidus	reduce 2',
    '	semicolon	reduce 2',
    '	less_than_operator	reduce 2',
    '	equals_operator	reduce 2',
    '	greater_than_operator	reduce 2',
    '	underscore	reduce 2',
    '	_ALTER	reduce 2',
    '	_AND	reduce 2',
    '	_AS	reduce 2',
    '	_AT	reduce 2',
    '	_BETWEEN	reduce 2',
    '	_CHECK	reduce 2',
    '	_COLLATE	reduce 2',
    '	_COMMIT	reduce 2',
    '	_CONNECT	reduce 2',
    '	_CONSTRAINT	reduce 2',
    '	_CREATE	reduce 2',
    '	_CROSS	reduce 2',
    '	_DAY	reduce 2',
    '	_DECLARE	reduce 2',
    '	_DELETE	reduce 2',
    '	_DISCONNECT	reduce 2',
    '	_DROP	reduce 2',
    '	_ELSE	reduce 2',
    '	_END	reduce 2',
    '	_ESCAPE	reduce 2',
    '	_EXCEPT	reduce 2',
    '	_FOR	reduce 2',
    '	_FROM	reduce 2',
    '	_FULL	reduce 2',
    '	_GRANT	reduce 2',
    '	_GROUP	reduce 2',
    '	_HAVING	reduce 2',
    '	_HOUR	reduce 2',
    '	_IN	reduce 2',
    '	_INNER	reduce 2',
    '	_INSERT	reduce 2',
    '	_INTERSECT	reduce 2',
    '	_INTO	reduce 2',
    '	_IS	reduce 2',
    '	_JOIN	reduce 2',
    '	_LEFT	reduce 2',
    '	_LIKE	reduce 2',
    '	_MATCH	reduce 2',
    '	_MINUTE	reduce 2',
    '	_MONTH	reduce 2',
    '	_NATURAL	reduce 2',
    '	_NOT	reduce 2',
    '	_OR	reduce 2',
    '	_ORDER	reduce 2',
    '	_OVERLAPS	reduce 2',
    '	_PRIMARY	reduce 2',
    '	_REFERENCES	reduce 2',
    '	_REVOKE	reduce 2',
    '	_RIGHT	reduce 2',
    '	_ROLLBACK	reduce 2',
    '	_SECOND	reduce 2',
    '	_SELECT	reduce 2',
    '	_SET	reduce 2',
    '	_TABLE	reduce 2',
    '	_THEN	reduce 2',
    '	_UNION	reduce 2',
    '	_UNIQUE	reduce 2',
    '	_UPDATE	reduce 2',
    '	_USER	reduce 2',
    '	_USING	reduce 2',
    '	_VALUES	reduce 2',
    '	_WHEN	reduce 2',
    '	_WHERE	reduce 2',
    '	_WITH	reduce 2',
    '	_YEAR	reduce 2',
    '	_E	reduce 12',
    '	.	error',
    '',
    'state 142:',
    '',
    '	signed_numeric_literal : unsigned_numeric_literal _	(204)',
    '',
    '	.	reduce 204',
    '',
    'state 143:',
    '',
    '	national_character_string_literal : national_character_string_literal_start _ national_character_string_literal_cont',
    '	national_character_string_literal_cont : _	(19)',
    '',
    '	.	reduce 19',
    '',
    '	national_character_string_literal_cont	goto 333',
    '',
    'state 144:',
    '',
    '	bit_string_literal : bit_string_literal_start _ bit_string_literal_cont',
    '	bit_string_literal_cont : _	(22)',
    '',
    '	.	reduce 22',
    '',
    '	bit_string_literal_cont	goto 334',
    '',
    'state 145:',
    '',
    '	character_string_literal_main : string_literal_continuation _	(29)',
    '',
    '	.	reduce 29',
    '',
    'state 146:',
    '',
    '	hex_string_literal : hex_string_literal_start _ hex_string_literal_cont',
    '	hex_string_literal_cont : _	(25)',
    '',
    '	.	reduce 25',
    '',
    '	hex_string_literal_cont	goto 335',
    '',
    'state 147:',
    '',
    '	unsigned_integer : digit _	(9)',
    '',
    '	.	reduce 9',
    '',
    'state 148:',
    '',
    '	sign : plus_sign _	(16)',
    '',
    '	.	reduce 16',
    '',
    'state 149:',
    '',
    '	sign : minus_sign _	(17)',
    '',
    '	.	reduce 17',
    '',
    'state 150:',
    '',
    '	exact_numeric_literal : period _ unsigned_integer',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	unsigned_integer	goto 336',
    '',
    'state 151:',
    '',
    '	parameter_name : colon _ identifier',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	actual_identifier	goto 61',
    '	identifier	goto 337',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 152:',
    '',
    '	disconnect_object : _ALL _	(870)',
    '',
    '	.	reduce 870',
    '',
    'state 153:',
    '',
    '	disconnect_object : _CURRENT _	(871)',
    '',
    '	.	reduce 871',
    '',
    'state 154:',
    '',
    '	date_literal : _DATE _ date_string',
    '',
    '	quote	shift 339',
    '	.	error',
    '',
    '	date_string	goto 338',
    '',
    'state 155:',
    '',
    '	connection_object : _DEFAULT _	(866)',
    '',
    '	.	reduce 866',
    '',
    'state 156:',
    '',
    '	interval_literal : _INTERVAL _ interval_string interval_qualifier',
    '	interval_literal : _INTERVAL _ sign interval_string interval_qualifier',
    '',
    '	quote	shift 342',
    '	plus_sign	shift 148',
    '	minus_sign	shift 149',
    '	.	error',
    '',
    '	interval_string	goto 340',
    '	sign	goto 341',
    '',
    'state 157:',
    '',
    '	time_literal : _TIME _ time_string',
    '',
    '	quote	shift 344',
    '	.	error',
    '',
    '	time_string	goto 343',
    '',
    'state 158:',
    '',
    '	timestamp_literal : _TIMESTAMP _ timestamp_string',
    '',
    '	quote	shift 346',
    '	.	error',
    '',
    '	timestamp_string	goto 345',
    '',
    'state 159:',
    '',
    '	drop_assertion_statement : _DROP _ASSERTION _ constraint_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	constraint_name	goto 347',
    '	qualified_name	goto 310',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 160:',
    '',
    '	drop_character_set_statement : _DROP _CHARACTER _ _SET character_set_name',
    '',
    '	_SET	shift 348',
    '	.	error',
    '',
    'state 161:',
    '',
    '	drop_collation_statement : _DROP _COLLATION _ collation_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	collation_name	goto 349',
    '	qualified_name	goto 313',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 162:',
    '',
    '	drop_domain_statement : _DROP _DOMAIN _ domain_name drop_behaviour',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	qualified_name	goto 301',
    '	domain_name	goto 350',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 163:',
    '',
    '	drop_schema_statement : _DROP _SCHEMA _ schema_name drop_behaviour',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	actual_identifier	goto 61',
    '	schema_name	goto 351',
    '	identifier	goto 319',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 164:',
    '',
    '	drop_table_statement : _DROP _TABLE _ table_name drop_behaviour',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	table_name	goto 352',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 165:',
    '',
    '	drop_translation_statement : _DROP _TRANSLATION _ translation_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	translation_name	goto 353',
    '	qualified_name	goto 322',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 166:',
    '',
    '	drop_view_statement : _DROP _VIEW _ table_name drop_behaviour',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	table_name	goto 354',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 167:',
    '',
    '	action_list : action _	(673)',
    '',
    '	.	reduce 673',
    '',
    'state 168:',
    '',
    '	privileges : action_list _	(672)',
    '	action_list : action_list _ comma action',
    '',
    '	comma	shift 355',
    '	_ON	reduce 672',
    '	.	error',
    '',
    'state 169:',
    '',
    '	grant_statement : _GRANT privileges _ _ON object_name _TO grantee_list grant_option',
    '',
    '	_ON	shift 356',
    '	.	error',
    '',
    'state 170:',
    '',
    '	privileges : _ALL _ _PRIVILEGES',
    '',
    '	_PRIVILEGES	shift 357',
    '	.	error',
    '',
    'state 171:',
    '',
    '	action : _DELETE _	(676)',
    '',
    '	.	reduce 676',
    '',
    'state 172:',
    '',
    '	action : _INSERT _ privilege_column_list_opt',
    '	privilege_column_list_opt : _	(681)',
    '',
    '	left_paren	shift 359',
    '	comma	reduce 681',
    '	_ON	reduce 681',
    '	.	error',
    '',
    '	privilege_column_list_opt	goto 358',
    '',
    'state 173:',
    '',
    '	action : _REFERENCES _ privilege_column_list_opt',
    '	privilege_column_list_opt : _	(681)',
    '',
    '	left_paren	shift 359',
    '	comma	reduce 681',
    '	_ON	reduce 681',
    '	.	error',
    '',
    '	privilege_column_list_opt	goto 360',
    '',
    'state 174:',
    '',
    '	action : _SELECT _	(675)',
    '',
    '	.	reduce 675',
    '',
    'state 175:',
    '',
    '	action : _UPDATE _ privilege_column_list_opt',
    '	privilege_column_list_opt : _	(681)',
    '',
    '	left_paren	shift 359',
    '	comma	reduce 681',
    '	_ON	reduce 681',
    '	.	error',
    '',
    '	privilege_column_list_opt	goto 361',
    '',
    'state 176:',
    '',
    '	action : _USAGE _	(680)',
    '',
    '	.	reduce 680',
    '',
    'state 177:',
    '',
    '	insert_statement : _INSERT _INTO _ table_name insert_columns_and_source',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	table_name	goto 362',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 178:',
    '',
    '	module_name_clause : _MODULE _MODULE _ module_name _MODULE module_character_set_specification _MODULE module_name module_character_set_specification',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	module_name	goto 363',
    '	actual_identifier	goto 61',
    '	identifier	goto 364',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 179:',
    '',
    '	revoke_statement : _REVOKE grant_option_for_opt _ privileges _ON object_name _FROM grantee_list drop_behaviour',
    '',
    '	_ALL	shift 170',
    '	_DELETE	shift 171',
    '	_INSERT	shift 172',
    '	_REFERENCES	shift 173',
    '	_SELECT	shift 174',
    '	_UPDATE	shift 175',
    '	_USAGE	shift 176',
    '	.	error',
    '',
    '	action	goto 167',
    '	action_list	goto 168',
    '	privileges	goto 365',
    '',
    'state 180:',
    '',
    '	grant_option_for_opt : _GRANT _ _OPTION _FOR',
    '',
    '	_OPTION	shift 366',
    '	.	error',
    '',
    'state 181:',
    '',
    '	rollback_statement : _ROLLBACK _WORK _	(851)',
    '',
    '	.	reduce 851',
    '',
    'state 182:',
    '',
    '	query_specification : _SELECT set_quantifier_opt _ select_list table_expression',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	asterisk	shift 375',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	derived_column	goto 367',
    '	select_sublist	goto 368',
    '	select_list_opt	goto 369',
    '	select_list	goto 370',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 371',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name_trail_asterisk	goto 372',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 373',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 183:',
    '',
    '	set_quantifier_opt : set_quantifier _	(350)',
    '',
    '	.	reduce 350',
    '',
    'state 184:',
    '',
    '	set_quantifier : _ALL _	(348)',
    '',
    '	.	reduce 348',
    '',
    'state 185:',
    '',
    '	set_quantifier : _DISTINCT _	(347)',
    '',
    '	.	reduce 347',
    '',
    'state 186:',
    '',
    '	set_catalog_statement : _SET _CATALOG _ value_specification',
    '',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	digit	shift 147',
    '	plus_sign	shift 148',
    '	minus_sign	shift 149',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_INTERVAL	shift 156',
    '	_SESSION_USER	shift 272',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	.	error',
    '',
    '	value_specification	goto 376',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 377',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 128',
    '	signed_numeric_literal	goto 129',
    '	literal	goto 378',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 132',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	sign	goto 137',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 142',
    '',
    'state 187:',
    '',
    '	set_connection_statement : _SET _CONNECTION _ connection_object',
    '',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	digit	shift 147',
    '	plus_sign	shift 148',
    '	minus_sign	shift 149',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 155',
    '	_INTERVAL	shift 156',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	.	error',
    '',
    '	connection_object	goto 379',
    '	connection_name	goto 120',
    '	simple_value_specification	goto 121',
    '	parameter_name	goto 122',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 128',
    '	signed_numeric_literal	goto 129',
    '	literal	goto 130',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 132',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	sign	goto 137',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 142',
    '',
    'state 188:',
    '',
    '	set_constraints_mode_statement : _SET _CONSTRAINTS _ constraint_name_list _DEFERRED',
    '	set_constraints_mode_statement : _SET _CONSTRAINTS _ constraint_name_list _IMMEDIATE',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_ALL	shift 383',
    '	.	error',
    '',
    '	constraint_name_list_some	goto 380',
    '	constraint_name_list	goto 381',
    '	constraint_name	goto 382',
    '	qualified_name	goto 310',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 189:',
    '',
    '	set_names_statement : _SET _NAMES _ value_specification',
    '',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	digit	shift 147',
    '	plus_sign	shift 148',
    '	minus_sign	shift 149',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_INTERVAL	shift 156',
    '	_SESSION_USER	shift 272',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	.	error',
    '',
    '	value_specification	goto 384',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 377',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 128',
    '	signed_numeric_literal	goto 129',
    '	literal	goto 378',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 132',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	sign	goto 137',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 142',
    '',
    'state 190:',
    '',
    '	set_schema_statement : _SET _SCHEMA _ value_specification',
    '',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	digit	shift 147',
    '	plus_sign	shift 148',
    '	minus_sign	shift 149',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_INTERVAL	shift 156',
    '	_SESSION_USER	shift 272',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	.	error',
    '',
    '	value_specification	goto 385',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 377',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 128',
    '	signed_numeric_literal	goto 129',
    '	literal	goto 378',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 132',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	sign	goto 137',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 142',
    '',
    'state 191:',
    '',
    '	set_session_authorization_identifier_statement : _SET _SESSION _ _AUTHORIZATION value_specification',
    '',
    '	_AUTHORIZATION	shift 386',
    '	.	error',
    '',
    'state 192:',
    '',
    '	set_local_time_zone_statement : _SET _TIME _ _ZONE set_time_zone_value',
    '',
    '	_ZONE	shift 387',
    '	.	error',
    '',
    'state 193:',
    '',
    '	set_transaction_statement : _SET _TRANSACTION _ transaction_mode_list',
    '',
    '	_DIAGNOSTICS	shift 393',
    '	_ISOLATION	shift 394',
    '	_READ	shift 395',
    '	.	error',
    '',
    '	diagnostics_size	goto 388',
    '	transaction_access_mode	goto 389',
    '	isolation_level	goto 390',
    '	transaction_mode	goto 391',
    '	transaction_mode_list	goto 392',
    '',
    'state 194:',
    '',
    '	explicit_table : _TABLE table_name _	(434)',
    '',
    '	.	reduce 434',
    '',
    'state 195:',
    '',
    '	table_name : qualified_name _	(246)',
    '',
    '	.	reduce 246',
    '',
    'state 196:',
    '',
    '	table_name : qualified_local_table_name _	(247)',
    '',
    '	.	reduce 247',
    '',
    'state 197:',
    '',
    '	qualified_name : identifier _	(187)',
    '	qualified_name : identifier _ period identifier',
    '	qualified_name : identifier _ period identifier period identifier',
    '',
    '	period	shift 396',
    '	$end	reduce 187',
    '	identifier_body	reduce 187',
    '	delimited_identifier	reduce 187',
    '	not_equals_operator	reduce 187',
    '	greater_than_or_equals_operator	reduce 187',
    '	less_than_or_equals_operator	reduce 187',
    '	concatenation_operator	reduce 187',
    '	quote	reduce 187',
    '	left_paren	reduce 187',
    '	right_paren	reduce 187',
    '	asterisk	reduce 187',
    '	plus_sign	reduce 187',
    '	comma	reduce 187',
    '	minus_sign	reduce 187',
    '	solidus	reduce 187',
    '	semicolon	reduce 187',
    '	less_than_operator	reduce 187',
    '	equals_operator	reduce 187',
    '	greater_than_operator	reduce 187',
    '	underscore	reduce 187',
    '	_ADD	reduce 187',
    '	_ALTER	reduce 187',
    '	_AND	reduce 187',
    '	_AS	reduce 187',
    '	_ASC	reduce 187',
    '	_AT	reduce 187',
    '	_BETWEEN	reduce 187',
    '	_BIT	reduce 187',
    '	_CASCADE	reduce 187',
    '	_CHAR	reduce 187',
    '	_CHARACTER	reduce 187',
    '	_CHECK	reduce 187',
    '	_COLLATE	reduce 187',
    '	_COMMIT	reduce 187',
    '	_CONNECT	reduce 187',
    '	_CONSTRAINT	reduce 187',
    '	_CREATE	reduce 187',
    '	_CROSS	reduce 187',
    '	_DATE	reduce 187',
    '	_DAY	reduce 187',
    '	_DEC	reduce 187',
    '	_DECIMAL	reduce 187',
    '	_DECLARE	reduce 187',
    '	_DEFAULT	reduce 187',
    '	_DEFERRABLE	reduce 187',
    '	_DEFERRED	reduce 187',
    '	_DELETE	reduce 187',
    '	_DESC	reduce 187',
    '	_DISCONNECT	reduce 187',
    '	_DOUBLE	reduce 187',
    '	_DROP	reduce 187',
    '	_ELSE	reduce 187',
    '	_END	reduce 187',
    '	_ESCAPE	reduce 187',
    '	_EXCEPT	reduce 187',
    '	_FLOAT	reduce 187',
    '	_FOR	reduce 187',
    '	_FOREIGN	reduce 187',
    '	_FROM	reduce 187',
    '	_FULL	reduce 187',
    '	_GRANT	reduce 187',
    '	_GROUP	reduce 187',
    '	_HAVING	reduce 187',
    '	_HOUR	reduce 187',
    '	_IMMEDIATE	reduce 187',
    '	_IN	reduce 187',
    '	_INITIALLY	reduce 187',
    '	_INNER	reduce 187',
    '	_INSERT	reduce 187',
    '	_INT	reduce 187',
    '	_INTEGER	reduce 187',
    '	_INTERSECT	reduce 187',
    '	_INTERVAL	reduce 187',
    '	_INTO	reduce 187',
    '	_IS	reduce 187',
    '	_JOIN	reduce 187',
    '	_LEFT	reduce 187',
    '	_LIKE	reduce 187',
    '	_MATCH	reduce 187',
    '	_MINUTE	reduce 187',
    '	_MONTH	reduce 187',
    '	_NATIONAL	reduce 187',
    '	_NATURAL	reduce 187',
    '	_NCHAR	reduce 187',
    '	_NO	reduce 187',
    '	_NOT	reduce 187',
    '	_NUMERIC	reduce 187',
    '	_ON	reduce 187',
    '	_OR	reduce 187',
    '	_ORDER	reduce 187',
    '	_OVERLAPS	reduce 187',
    '	_PAD	reduce 187',
    '	_PRIMARY	reduce 187',
    '	_REAL	reduce 187',
    '	_REFERENCES	reduce 187',
    '	_RESTRICT	reduce 187',
    '	_REVOKE	reduce 187',
    '	_RIGHT	reduce 187',
    '	_ROLLBACK	reduce 187',
    '	_SECOND	reduce 187',
    '	_SELECT	reduce 187',
    '	_SET	reduce 187',
    '	_SMALLINT	reduce 187',
    '	_TABLE	reduce 187',
    '	_THEN	reduce 187',
    '	_TIME	reduce 187',
    '	_TIMESTAMP	reduce 187',
    '	_TO	reduce 187',
    '	_UNION	reduce 187',
    '	_UNIQUE	reduce 187',
    '	_UPDATE	reduce 187',
    '	_USING	reduce 187',
    '	_VALUES	reduce 187',
    '	_VARCHAR	reduce 187',
    '	_WHEN	reduce 187',
    '	_WHERE	reduce 187',
    '	_WITH	reduce 187',
    '	_YEAR	reduce 187',
    '	.	error',
    '',
    'state 198:',
    '',
    '	qualified_local_table_name : _MODULE _ period local_table_name',
    '',
    '	period	shift 397',
    '	.	error',
    '',
    'state 199:',
    '',
    '	update_statement__searched : _UPDATE table_name _ _SET set_clause_list where_clause_opt',
    '',
    '	_SET	shift 398',
    '	.	error',
    '',
    'state 200:',
    '',
    '	char_length_expression : char_length_specifier _ left_paren expression right_paren',
    '',
    '	left_paren	shift 399',
    '	.	error',
    '',
    'state 201:',
    '',
    '	length_expression : bit_length_expression _	(506)',
    '',
    '	.	reduce 506',
    '',
    'state 202:',
    '',
    '	length_expression : octet_length_expression _	(505)',
    '',
    '	.	reduce 505',
    '',
    'state 203:',
    '',
    '	length_expression : char_length_expression _	(504)',
    '',
    '	.	reduce 504',
    '',
    'state 204:',
    '',
    '	character_value_function : trim_function _	(471)',
    '',
    '	.	reduce 471',
    '',
    'state 205:',
    '',
    '	character_value_function : character_translation _	(470)',
    '',
    '	.	reduce 470',
    '',
    'state 206:',
    '',
    '	character_value_function : form_of_use_conversion _	(469)',
    '',
    '	.	reduce 469',
    '',
    'state 207:',
    '',
    '	character_value_function : fold _	(468)',
    '',
    '	.	reduce 468',
    '',
    'state 208:',
    '',
    '	character_value_function : character_bit_substring_function _	(467)',
    '',
    '	.	reduce 467',
    '',
    'state 209:',
    '',
    '	numeric_value_function : length_expression _	(465)',
    '',
    '	.	reduce 465',
    '',
    'state 210:',
    '',
    '	numeric_value_function : extract_expression _	(464)',
    '',
    '	.	reduce 464',
    '',
    'state 211:',
    '',
    '	numeric_value_function : position_expression _	(463)',
    '',
    '	.	reduce 463',
    '',
    'state 212:',
    '',
    '	case_specification : searched_case _	(448)',
    '',
    '	.	reduce 448',
    '',
    'state 213:',
    '',
    '	case_specification : simple_case _	(447)',
    '',
    '	.	reduce 447',
    '',
    'state 214:',
    '',
    '	case_expression : case_specification _	(442)',
    '',
    '	.	reduce 442',
    '',
    'state 215:',
    '',
    '	case_expression : case_abbreviation _	(441)',
    '',
    '	.	reduce 441',
    '',
    'state 216:',
    '',
    '	table_value_constructor : _VALUES table_value_constructor_list _	(431)',
    '	table_value_constructor_list : table_value_constructor_list _ comma row_value_constructor',
    '',
    '	comma	shift 400',
    '	$end	reduce 431',
    '	identifier_body	reduce 431',
    '	delimited_identifier	reduce 431',
    '	left_paren	reduce 431',
    '	right_paren	reduce 431',
    '	semicolon	reduce 431',
    '	underscore	reduce 431',
    '	_ALTER	reduce 431',
    '	_COMMIT	reduce 431',
    '	_CONNECT	reduce 431',
    '	_CREATE	reduce 431',
    '	_DECLARE	reduce 431',
    '	_DELETE	reduce 431',
    '	_DISCONNECT	reduce 431',
    '	_DROP	reduce 431',
    '	_EXCEPT	reduce 431',
    '	_FOR	reduce 431',
    '	_GRANT	reduce 431',
    '	_INSERT	reduce 431',
    '	_INTERSECT	reduce 431',
    '	_ORDER	reduce 431',
    '	_REVOKE	reduce 431',
    '	_ROLLBACK	reduce 431',
    '	_SELECT	reduce 431',
    '	_SET	reduce 431',
    '	_TABLE	reduce 431',
    '	_UNION	reduce 431',
    '	_UPDATE	reduce 431',
    '	_VALUES	reduce 431',
    '	_WITH	reduce 431',
    '	.	error',
    '',
    'state 217:',
    '',
    '	general_set_function : set_function_type _ left_paren set_quantifier_args right_paren',
    '',
    '	left_paren	shift 401',
    '	.	error',
    '',
    'state 218:',
    '',
    '	set_function_specification : general_set_function _	(337)',
    '',
    '	.	reduce 337',
    '',
    'state 219:',
    '',
    '	parameter_specification : parameter_name _ indicator_parameter_opt',
    '	indicator_parameter_opt : _	(332)',
    '',
    '	colon	shift 151',
    '	_INDICATOR	shift 404',
    '	$end	reduce 332',
    '	identifier_body	reduce 332',
    '	delimited_identifier	reduce 332',
    '	not_equals_operator	reduce 332',
    '	greater_than_or_equals_operator	reduce 332',
    '	less_than_or_equals_operator	reduce 332',
    '	concatenation_operator	reduce 332',
    '	left_paren	reduce 332',
    '	right_paren	reduce 332',
    '	asterisk	reduce 332',
    '	plus_sign	reduce 332',
    '	comma	reduce 332',
    '	minus_sign	reduce 332',
    '	solidus	reduce 332',
    '	semicolon	reduce 332',
    '	less_than_operator	reduce 332',
    '	equals_operator	reduce 332',
    '	greater_than_operator	reduce 332',
    '	underscore	reduce 332',
    '	_ALTER	reduce 332',
    '	_AND	reduce 332',
    '	_AS	reduce 332',
    '	_AT	reduce 332',
    '	_BETWEEN	reduce 332',
    '	_COLLATE	reduce 332',
    '	_COMMIT	reduce 332',
    '	_CONNECT	reduce 332',
    '	_CREATE	reduce 332',
    '	_CROSS	reduce 332',
    '	_DAY	reduce 332',
    '	_DECLARE	reduce 332',
    '	_DELETE	reduce 332',
    '	_DISCONNECT	reduce 332',
    '	_DROP	reduce 332',
    '	_ELSE	reduce 332',
    '	_END	reduce 332',
    '	_ESCAPE	reduce 332',
    '	_EXCEPT	reduce 332',
    '	_FOR	reduce 332',
    '	_FROM	reduce 332',
    '	_FULL	reduce 332',
    '	_GRANT	reduce 332',
    '	_GROUP	reduce 332',
    '	_HAVING	reduce 332',
    '	_HOUR	reduce 332',
    '	_IN	reduce 332',
    '	_INNER	reduce 332',
    '	_INSERT	reduce 332',
    '	_INTERSECT	reduce 332',
    '	_INTO	reduce 332',
    '	_IS	reduce 332',
    '	_JOIN	reduce 332',
    '	_LEFT	reduce 332',
    '	_LIKE	reduce 332',
    '	_MATCH	reduce 332',
    '	_MINUTE	reduce 332',
    '	_MONTH	reduce 332',
    '	_NATURAL	reduce 332',
    '	_NOT	reduce 332',
    '	_OR	reduce 332',
    '	_ORDER	reduce 332',
    '	_OVERLAPS	reduce 332',
    '	_REVOKE	reduce 332',
    '	_RIGHT	reduce 332',
    '	_ROLLBACK	reduce 332',
    '	_SECOND	reduce 332',
    '	_SELECT	reduce 332',
    '	_SET	reduce 332',
    '	_TABLE	reduce 332',
    '	_THEN	reduce 332',
    '	_UNION	reduce 332',
    '	_UPDATE	reduce 332',
    '	_USING	reduce 332',
    '	_VALUES	reduce 332',
    '	_WHEN	reduce 332',
    '	_WHERE	reduce 332',
    '	_WITH	reduce 332',
    '	_YEAR	reduce 332',
    '	.	error',
    '',
    '	indicator_parameter_opt	goto 402',
    '	parameter_name	goto 403',
    '',
    'state 220:',
    '',
    '	general_value_specification : parameter_specification _	(324)',
    '',
    '	.	reduce 324',
    '',
    'state 221:',
    '',
    '	unsigned_value_specification : general_value_specification _	(321)',
    '',
    '	.	reduce 321',
    '',
    'state 222:',
    '',
    '	unsigned_value_specification : unsigned_literal _	(320)',
    '',
    '	.	reduce 320',
    '',
    'state 223:',
    '',
    '	string_value_function : character_value_function _	(317)',
    '',
    '	.	reduce 317',
    '',
    'state 224:',
    '',
    '	*** conflicts:',
    '',
    '	shift 405, reduce 313 on asterisk',
    '	shift 406, reduce 313 on solidus',
    '',
    '	multiplicative_expression : multiplicative_expression _ asterisk unary_expression',
    '	multiplicative_expression : multiplicative_expression _ solidus unary_expression',
    '	expression : multiplicative_expression _	(313)',
    '',
    '	asterisk	shift 405',
    '	solidus	shift 406',
    '	$end	reduce 313',
    '	identifier_body	reduce 313',
    '	delimited_identifier	reduce 313',
    '	not_equals_operator	reduce 313',
    '	greater_than_or_equals_operator	reduce 313',
    '	less_than_or_equals_operator	reduce 313',
    '	concatenation_operator	reduce 313',
    '	left_paren	reduce 313',
    '	right_paren	reduce 313',
    '	plus_sign	reduce 313',
    '	comma	reduce 313',
    '	minus_sign	reduce 313',
    '	semicolon	reduce 313',
    '	less_than_operator	reduce 313',
    '	equals_operator	reduce 313',
    '	greater_than_operator	reduce 313',
    '	underscore	reduce 313',
    '	_ALTER	reduce 313',
    '	_AND	reduce 313',
    '	_AS	reduce 313',
    '	_BETWEEN	reduce 313',
    '	_COMMIT	reduce 313',
    '	_CONNECT	reduce 313',
    '	_CREATE	reduce 313',
    '	_CROSS	reduce 313',
    '	_DECLARE	reduce 313',
    '	_DELETE	reduce 313',
    '	_DISCONNECT	reduce 313',
    '	_DROP	reduce 313',
    '	_ELSE	reduce 313',
    '	_END	reduce 313',
    '	_ESCAPE	reduce 313',
    '	_EXCEPT	reduce 313',
    '	_FOR	reduce 313',
    '	_FROM	reduce 313',
    '	_FULL	reduce 313',
    '	_GRANT	reduce 313',
    '	_GROUP	reduce 313',
    '	_HAVING	reduce 313',
    '	_IN	reduce 313',
    '	_INNER	reduce 313',
    '	_INSERT	reduce 313',
    '	_INTERSECT	reduce 313',
    '	_INTO	reduce 313',
    '	_IS	reduce 313',
    '	_JOIN	reduce 313',
    '	_LEFT	reduce 313',
    '	_LIKE	reduce 313',
    '	_MATCH	reduce 313',
    '	_NATURAL	reduce 313',
    '	_NOT	reduce 313',
    '	_OR	reduce 313',
    '	_ORDER	reduce 313',
    '	_OVERLAPS	reduce 313',
    '	_REVOKE	reduce 313',
    '	_RIGHT	reduce 313',
    '	_ROLLBACK	reduce 313',
    '	_SELECT	reduce 313',
    '	_SET	reduce 313',
    '	_TABLE	reduce 313',
    '	_THEN	reduce 313',
    '	_UNION	reduce 313',
    '	_UPDATE	reduce 313',
    '	_USING	reduce 313',
    '	_VALUES	reduce 313',
    '	_WHEN	reduce 313',
    '	_WHERE	reduce 313',
    '	_WITH	reduce 313',
    '	.	error',
    '',
    'state 225:',
    '',
    '	multiplicative_expression : unary_expression _	(310)',
    '',
    '	.	reduce 310',
    '',
    'state 226:',
    '',
    '	unary_expression : postfix_expression _	(309)',
    '',
    '	.	reduce 309',
    '',
    'state 227:',
    '',
    '	primary_expression : default_specification _	(300)',
    '',
    '	.	reduce 300',
    '',
    'state 228:',
    '',
    '	primary_expression : null_specification _	(299)',
    '',
    '	.	reduce 299',
    '',
    'state 229:',
    '',
    '	primary_expression : string_value_function _	(297)',
    '',
    '	.	reduce 297',
    '',
    'state 230:',
    '',
    '	primary_expression : numeric_value_function _	(296)',
    '',
    '	.	reduce 296',
    '',
    'state 231:',
    '',
    '	primary_expression : cast_specification _	(295)',
    '',
    '	.	reduce 295',
    '',
    'state 232:',
    '',
    '	primary_expression : case_expression _	(294)',
    '',
    '	.	reduce 294',
    '',
    'state 233:',
    '',
    '	primary_expression : scalar_subquery _	(293)',
    '',
    '	.	reduce 293',
    '',
    'state 234:',
    '',
    '	primary_expression : set_function_specification _	(292)',
    '',
    '	.	reduce 292',
    '',
    'state 235:',
    '',
    '	primary_expression : column_reference _	(291)',
    '',
    '	.	reduce 291',
    '',
    'state 236:',
    '',
    '	primary_expression : unsigned_value_specification _	(290)',
    '',
    '	.	reduce 290',
    '',
    'state 237:',
    '',
    '	postfix_expression : primary_expression _	(302)',
    '	postfix_expression : primary_expression _ postfix_op',
    '',
    '	_AT	shift 413',
    '	_COLLATE	shift 414',
    '	_DAY	shift 415',
    '	_HOUR	shift 416',
    '	_MINUTE	shift 417',
    '	_MONTH	shift 418',
    '	_SECOND	shift 419',
    '	_YEAR	shift 420',
    '	$end	reduce 302',
    '	identifier_body	reduce 302',
    '	delimited_identifier	reduce 302',
    '	not_equals_operator	reduce 302',
    '	greater_than_or_equals_operator	reduce 302',
    '	less_than_or_equals_operator	reduce 302',
    '	concatenation_operator	reduce 302',
    '	left_paren	reduce 302',
    '	right_paren	reduce 302',
    '	asterisk	reduce 302',
    '	plus_sign	reduce 302',
    '	comma	reduce 302',
    '	minus_sign	reduce 302',
    '	solidus	reduce 302',
    '	semicolon	reduce 302',
    '	less_than_operator	reduce 302',
    '	equals_operator	reduce 302',
    '	greater_than_operator	reduce 302',
    '	underscore	reduce 302',
    '	_ALTER	reduce 302',
    '	_AND	reduce 302',
    '	_AS	reduce 302',
    '	_BETWEEN	reduce 302',
    '	_COMMIT	reduce 302',
    '	_CONNECT	reduce 302',
    '	_CREATE	reduce 302',
    '	_CROSS	reduce 302',
    '	_DECLARE	reduce 302',
    '	_DELETE	reduce 302',
    '	_DISCONNECT	reduce 302',
    '	_DROP	reduce 302',
    '	_ELSE	reduce 302',
    '	_END	reduce 302',
    '	_ESCAPE	reduce 302',
    '	_EXCEPT	reduce 302',
    '	_FOR	reduce 302',
    '	_FROM	reduce 302',
    '	_FULL	reduce 302',
    '	_GRANT	reduce 302',
    '	_GROUP	reduce 302',
    '	_HAVING	reduce 302',
    '	_IN	reduce 302',
    '	_INNER	reduce 302',
    '	_INSERT	reduce 302',
    '	_INTERSECT	reduce 302',
    '	_INTO	reduce 302',
    '	_IS	reduce 302',
    '	_JOIN	reduce 302',
    '	_LEFT	reduce 302',
    '	_LIKE	reduce 302',
    '	_MATCH	reduce 302',
    '	_NATURAL	reduce 302',
    '	_NOT	reduce 302',
    '	_OR	reduce 302',
    '	_ORDER	reduce 302',
    '	_OVERLAPS	reduce 302',
    '	_REVOKE	reduce 302',
    '	_RIGHT	reduce 302',
    '	_ROLLBACK	reduce 302',
    '	_SELECT	reduce 302',
    '	_SET	reduce 302',
    '	_TABLE	reduce 302',
    '	_THEN	reduce 302',
    '	_UNION	reduce 302',
    '	_UPDATE	reduce 302',
    '	_USING	reduce 302',
    '	_VALUES	reduce 302',
    '	_WHEN	reduce 302',
    '	_WHERE	reduce 302',
    '	_WITH	reduce 302',
    '	.	error',
    '',
    '	time_zone	goto 407',
    '	postfix_op	goto 408',
    '	non_second_datetime_field	goto 409',
    '	start_field	goto 410',
    '	interval_qualifier	goto 411',
    '	collate_clause	goto 412',
    '',
    'state 238:',
    '',
    '	row_value_constructor : expression _	(288)',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	$end	reduce 288',
    '	identifier_body	reduce 288',
    '	delimited_identifier	reduce 288',
    '	left_paren	reduce 288',
    '	right_paren	reduce 288',
    '	comma	reduce 288',
    '	semicolon	reduce 288',
    '	underscore	reduce 288',
    '	_ALTER	reduce 288',
    '	_AND	reduce 288',
    '	_COMMIT	reduce 288',
    '	_CONNECT	reduce 288',
    '	_CREATE	reduce 288',
    '	_CROSS	reduce 288',
    '	_DECLARE	reduce 288',
    '	_DELETE	reduce 288',
    '	_DISCONNECT	reduce 288',
    '	_DROP	reduce 288',
    '	_EXCEPT	reduce 288',
    '	_FOR	reduce 288',
    '	_FULL	reduce 288',
    '	_GRANT	reduce 288',
    '	_GROUP	reduce 288',
    '	_HAVING	reduce 288',
    '	_INNER	reduce 288',
    '	_INSERT	reduce 288',
    '	_INTERSECT	reduce 288',
    '	_IS	reduce 288',
    '	_JOIN	reduce 288',
    '	_LEFT	reduce 288',
    '	_NATURAL	reduce 288',
    '	_OR	reduce 288',
    '	_ORDER	reduce 288',
    '	_REVOKE	reduce 288',
    '	_RIGHT	reduce 288',
    '	_ROLLBACK	reduce 288',
    '	_SELECT	reduce 288',
    '	_SET	reduce 288',
    '	_TABLE	reduce 288',
    '	_THEN	reduce 288',
    '	_UNION	reduce 288',
    '	_UPDATE	reduce 288',
    '	_VALUES	reduce 288',
    '	_WHERE	reduce 288',
    '	_WITH	reduce 288',
    '	.	error',
    '',
    'state 239:',
    '',
    '	table_value_constructor_list : row_value_constructor _	(432)',
    '',
    '	.	reduce 432',
    '',
    'state 240:',
    '',
    '	datetime_value_function : current_timestamp_value_function _	(221)',
    '',
    '	.	reduce 221',
    '',
    'state 241:',
    '',
    '	datetime_value_function : current_time_value_function _	(220)',
    '',
    '	.	reduce 220',
    '',
    'state 242:',
    '',
    '	datetime_value_function : current_date_value_function _	(219)',
    '',
    '	.	reduce 219',
    '',
    'state 243:',
    '',
    '	unsigned_literal : general_literal _	(323)',
    '',
    '	.	reduce 323',
    '',
    'state 244:',
    '',
    '	primary_expression : datetime_value_function _	(298)',
    '',
    '	.	reduce 298',
    '',
    'state 245:',
    '',
    '	column_reference : qualified_name _	(335)',
    '',
    '	.	reduce 335',
    '',
    'state 246:',
    '',
    '	character_string_literal : introducer _ character_set_specification character_string_literal_main',
    '	identifier : introducer _ character_set_specification actual_identifier',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	actual_identifier	goto 61',
    '	SQL_language_identifier	goto 96',
    '	identifier	goto 97',
    '	character_set_name	goto 98',
    '	character_set_specification	goto 424',
    '	introducer	goto 63',
    '	regular_identifier	goto 100',
    '',
    'state 247:',
    '',
    '	unsigned_literal : unsigned_numeric_literal _	(322)',
    '',
    '	.	reduce 322',
    '',
    'state 248:',
    '',
    '	row_value_constructor : left_paren _ row_value_constructor_list right_paren',
    '	primary_expression : left_paren _ expression right_paren',
    '	scalar_subquery : left_paren _ subquery right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 429',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SELECT	shift 83',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TABLE	shift 85',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_VALUES	shift 87',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	explicit_table	goto 49',
    '	table_value_constructor	goto 50',
    '	query_specification	goto 51',
    '	table_subquery	goto 52',
    '	simple_table	goto 53',
    '	non_join_query_primary	goto 54',
    '	query_term	goto 55',
    '	non_join_query_term	goto 56',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	query_expression	goto 425',
    '	subquery	goto 426',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	row_value_constructor_list	goto 427',
    '	expression	goto 428',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 249:',
    '',
    '	unary_expression : plus_sign _ postfix_expression',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	postfix_expression	goto 430',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 250:',
    '',
    '	unary_expression : minus_sign _ postfix_expression',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	postfix_expression	goto 431',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 251:',
    '',
    '	set_function_type : _AVG _	(342)',
    '',
    '	.	reduce 342',
    '',
    'state 252:',
    '',
    '	bit_length_expression : _BIT_LENGTH _ left_paren expression right_paren',
    '',
    '	left_paren	shift 432',
    '	.	error',
    '',
    'state 253:',
    '',
    '	simple_case : _CASE _ case_operand simple_when_clause else_clause_opt _END',
    '	searched_case : _CASE _ searched_when_clause else_clause_opt _END',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_WHEN	shift 436',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_when_clause	goto 433',
    '	case_operand	goto 434',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 435',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 254:',
    '',
    '	cast_specification : _CAST _ left_paren cast_operand _AS cast_target right_paren',
    '',
    '	left_paren	shift 437',
    '	.	error',
    '',
    'state 255:',
    '',
    '	char_length_specifier : _CHARACTER_LENGTH _	(509)',
    '',
    '	.	reduce 509',
    '',
    'state 256:',
    '',
    '	char_length_specifier : _CHAR_LENGTH _	(508)',
    '',
    '	.	reduce 508',
    '',
    'state 257:',
    '',
    '	case_abbreviation : _COALESCE _ left_paren expression_list right_paren',
    '',
    '	left_paren	shift 438',
    '	.	error',
    '',
    'state 258:',
    '',
    '	form_of_use_conversion : _CONVERT _ left_paren expression _USING form_of_use_conversion_name right_paren',
    '',
    '	left_paren	shift 439',
    '	.	error',
    '',
    'state 259:',
    '',
    '	current_date_value_function : _CURRENT_DATE _	(222)',
    '',
    '	.	reduce 222',
    '',
    'state 260:',
    '',
    '	*** conflicts:',
    '',
    '	shift 440, reduce 223 on left_paren',
    '',
    '	current_time_value_function : _CURRENT_TIME _	(223)',
    '	current_time_value_function : _CURRENT_TIME _ left_paren time_precision right_paren',
    '',
    '	left_paren	shift 440',
    '	$end	reduce 223',
    '	identifier_body	reduce 223',
    '	delimited_identifier	reduce 223',
    '	not_equals_operator	reduce 223',
    '	greater_than_or_equals_operator	reduce 223',
    '	less_than_or_equals_operator	reduce 223',
    '	concatenation_operator	reduce 223',
    '	right_paren	reduce 223',
    '	asterisk	reduce 223',
    '	plus_sign	reduce 223',
    '	comma	reduce 223',
    '	minus_sign	reduce 223',
    '	solidus	reduce 223',
    '	semicolon	reduce 223',
    '	less_than_operator	reduce 223',
    '	equals_operator	reduce 223',
    '	greater_than_operator	reduce 223',
    '	underscore	reduce 223',
    '	_ALTER	reduce 223',
    '	_AND	reduce 223',
    '	_AS	reduce 223',
    '	_AT	reduce 223',
    '	_BETWEEN	reduce 223',
    '	_CHECK	reduce 223',
    '	_COLLATE	reduce 223',
    '	_COMMIT	reduce 223',
    '	_CONNECT	reduce 223',
    '	_CONSTRAINT	reduce 223',
    '	_CREATE	reduce 223',
    '	_CROSS	reduce 223',
    '	_DAY	reduce 223',
    '	_DECLARE	reduce 223',
    '	_DELETE	reduce 223',
    '	_DISCONNECT	reduce 223',
    '	_DROP	reduce 223',
    '	_ELSE	reduce 223',
    '	_END	reduce 223',
    '	_ESCAPE	reduce 223',
    '	_EXCEPT	reduce 223',
    '	_FOR	reduce 223',
    '	_FROM	reduce 223',
    '	_FULL	reduce 223',
    '	_GRANT	reduce 223',
    '	_GROUP	reduce 223',
    '	_HAVING	reduce 223',
    '	_HOUR	reduce 223',
    '	_IN	reduce 223',
    '	_INNER	reduce 223',
    '	_INSERT	reduce 223',
    '	_INTERSECT	reduce 223',
    '	_INTO	reduce 223',
    '	_IS	reduce 223',
    '	_JOIN	reduce 223',
    '	_LEFT	reduce 223',
    '	_LIKE	reduce 223',
    '	_MATCH	reduce 223',
    '	_MINUTE	reduce 223',
    '	_MONTH	reduce 223',
    '	_NATURAL	reduce 223',
    '	_NOT	reduce 223',
    '	_OR	reduce 223',
    '	_ORDER	reduce 223',
    '	_OVERLAPS	reduce 223',
    '	_PRIMARY	reduce 223',
    '	_REFERENCES	reduce 223',
    '	_REVOKE	reduce 223',
    '	_RIGHT	reduce 223',
    '	_ROLLBACK	reduce 223',
    '	_SECOND	reduce 223',
    '	_SELECT	reduce 223',
    '	_SET	reduce 223',
    '	_TABLE	reduce 223',
    '	_THEN	reduce 223',
    '	_UNION	reduce 223',
    '	_UNIQUE	reduce 223',
    '	_UPDATE	reduce 223',
    '	_USING	reduce 223',
    '	_VALUES	reduce 223',
    '	_WHEN	reduce 223',
    '	_WHERE	reduce 223',
    '	_WITH	reduce 223',
    '	_YEAR	reduce 223',
    '	.	error',
    '',
    'state 261:',
    '',
    '	*** conflicts:',
    '',
    '	shift 441, reduce 225 on left_paren',
    '',
    '	current_timestamp_value_function : _CURRENT_TIMESTAMP _	(225)',
    '	current_timestamp_value_function : _CURRENT_TIMESTAMP _ left_paren timestamp_precision right_paren',
    '',
    '	left_paren	shift 441',
    '	$end	reduce 225',
    '	identifier_body	reduce 225',
    '	delimited_identifier	reduce 225',
    '	not_equals_operator	reduce 225',
    '	greater_than_or_equals_operator	reduce 225',
    '	less_than_or_equals_operator	reduce 225',
    '	concatenation_operator	reduce 225',
    '	right_paren	reduce 225',
    '	asterisk	reduce 225',
    '	plus_sign	reduce 225',
    '	comma	reduce 225',
    '	minus_sign	reduce 225',
    '	solidus	reduce 225',
    '	semicolon	reduce 225',
    '	less_than_operator	reduce 225',
    '	equals_operator	reduce 225',
    '	greater_than_operator	reduce 225',
    '	underscore	reduce 225',
    '	_ALTER	reduce 225',
    '	_AND	reduce 225',
    '	_AS	reduce 225',
    '	_AT	reduce 225',
    '	_BETWEEN	reduce 225',
    '	_CHECK	reduce 225',
    '	_COLLATE	reduce 225',
    '	_COMMIT	reduce 225',
    '	_CONNECT	reduce 225',
    '	_CONSTRAINT	reduce 225',
    '	_CREATE	reduce 225',
    '	_CROSS	reduce 225',
    '	_DAY	reduce 225',
    '	_DECLARE	reduce 225',
    '	_DELETE	reduce 225',
    '	_DISCONNECT	reduce 225',
    '	_DROP	reduce 225',
    '	_ELSE	reduce 225',
    '	_END	reduce 225',
    '	_ESCAPE	reduce 225',
    '	_EXCEPT	reduce 225',
    '	_FOR	reduce 225',
    '	_FROM	reduce 225',
    '	_FULL	reduce 225',
    '	_GRANT	reduce 225',
    '	_GROUP	reduce 225',
    '	_HAVING	reduce 225',
    '	_HOUR	reduce 225',
    '	_IN	reduce 225',
    '	_INNER	reduce 225',
    '	_INSERT	reduce 225',
    '	_INTERSECT	reduce 225',
    '	_INTO	reduce 225',
    '	_IS	reduce 225',
    '	_JOIN	reduce 225',
    '	_LEFT	reduce 225',
    '	_LIKE	reduce 225',
    '	_MATCH	reduce 225',
    '	_MINUTE	reduce 225',
    '	_MONTH	reduce 225',
    '	_NATURAL	reduce 225',
    '	_NOT	reduce 225',
    '	_OR	reduce 225',
    '	_ORDER	reduce 225',
    '	_OVERLAPS	reduce 225',
    '	_PRIMARY	reduce 225',
    '	_REFERENCES	reduce 225',
    '	_REVOKE	reduce 225',
    '	_RIGHT	reduce 225',
    '	_ROLLBACK	reduce 225',
    '	_SECOND	reduce 225',
    '	_SELECT	reduce 225',
    '	_SET	reduce 225',
    '	_TABLE	reduce 225',
    '	_THEN	reduce 225',
    '	_UNION	reduce 225',
    '	_UNIQUE	reduce 225',
    '	_UPDATE	reduce 225',
    '	_USING	reduce 225',
    '	_VALUES	reduce 225',
    '	_WHEN	reduce 225',
    '	_WHERE	reduce 225',
    '	_WITH	reduce 225',
    '	_YEAR	reduce 225',
    '	.	error',
    '',
    'state 262:',
    '',
    '	general_value_specification : _CURRENT_USER _	(326)',
    '',
    '	.	reduce 326',
    '',
    'state 263:',
    '',
    '	default_specification : _DEFAULT _	(513)',
    '',
    '	.	reduce 513',
    '',
    'state 264:',
    '',
    '	extract_expression : _EXTRACT _ left_paren extract_field _FROM extract_source right_paren',
    '',
    '	left_paren	shift 442',
    '	.	error',
    '',
    'state 265:',
    '',
    '	fold : _LOWER _ left_paren expression right_paren',
    '',
    '	left_paren	shift 443',
    '	.	error',
    '',
    'state 266:',
    '',
    '	set_function_type : _MAX _	(343)',
    '',
    '	.	reduce 343',
    '',
    'state 267:',
    '',
    '	set_function_type : _MIN _	(344)',
    '',
    '	.	reduce 344',
    '',
    'state 268:',
    '',
    '	null_specification : _NULL _	(512)',
    '',
    '	.	reduce 512',
    '',
    'state 269:',
    '',
    '	case_abbreviation : _NULLIF _ left_paren expression comma expression right_paren',
    '',
    '	left_paren	shift 444',
    '	.	error',
    '',
    'state 270:',
    '',
    '	octet_length_expression : _OCTET_LENGTH _ left_paren expression right_paren',
    '',
    '	left_paren	shift 445',
    '	.	error',
    '',
    'state 271:',
    '',
    '	position_expression : _POSITION _ left_paren expression _IN expression right_paren',
    '',
    '	left_paren	shift 446',
    '	.	error',
    '',
    'state 272:',
    '',
    '	general_value_specification : _SESSION_USER _	(327)',
    '',
    '	.	reduce 327',
    '',
    'state 273:',
    '',
    '	character_bit_substring_function : _SUBSTRING _ left_paren expression _FROM start_position for_strlength_opt right_paren',
    '',
    '	left_paren	shift 447',
    '	.	error',
    '',
    'state 274:',
    '',
    '	set_function_type : _SUM _	(345)',
    '',
    '	.	reduce 345',
    '',
    'state 275:',
    '',
    '	general_value_specification : _SYSTEM_USER _	(328)',
    '',
    '	.	reduce 328',
    '',
    'state 276:',
    '',
    '	character_translation : _TRANSLATE _ left_paren expression _USING translation_name right_paren',
    '',
    '	left_paren	shift 448',
    '	.	error',
    '',
    'state 277:',
    '',
    '	trim_function : _TRIM _ left_paren trim_operands right_paren',
    '',
    '	left_paren	shift 449',
    '	.	error',
    '',
    'state 278:',
    '',
    '	fold : _UPPER _ left_paren expression right_paren',
    '',
    '	left_paren	shift 450',
    '	.	error',
    '',
    'state 279:',
    '',
    '	general_value_specification : _USER _	(325)',
    '',
    '	.	reduce 325',
    '',
    'state 280:',
    '',
    '	general_value_specification : _VALUE _	(329)',
    '',
    '	.	reduce 329',
    '',
    'state 281:',
    '',
    '	set_function_type : _COUNT _	(346)',
    '',
    '	.	reduce 346',
    '',
    'state 282:',
    '',
    '	non_join_query_term : query_term _INTERSECT all_opt _ corresponding_spec_opt query_primary',
    '	corresponding_spec_opt : _	(358)',
    '',
    '	_CORRESPONDING	shift 453',
    '	left_paren	reduce 358',
    '	_SELECT	reduce 358',
    '	_TABLE	reduce 358',
    '	_VALUES	reduce 358',
    '	.	error',
    '',
    '	corresponding_spec	goto 451',
    '	corresponding_spec_opt	goto 452',
    '',
    'state 283:',
    '',
    '	all_opt : _ALL _	(357)',
    '',
    '	.	reduce 357',
    '',
    'state 284:',
    '',
    '	query_expression : query_expression _EXCEPT all_opt _ corresponding_spec_opt query_term',
    '	corresponding_spec_opt : _	(358)',
    '',
    '	_CORRESPONDING	shift 453',
    '	left_paren	reduce 358',
    '	_SELECT	reduce 358',
    '	_TABLE	reduce 358',
    '	_VALUES	reduce 358',
    '	.	error',
    '',
    '	corresponding_spec	goto 451',
    '	corresponding_spec_opt	goto 454',
    '',
    'state 285:',
    '',
    '	order_by_clause_opt : _ORDER _BY _ sort_specification_list',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	sort_key	goto 455',
    '	sort_specification	goto 456',
    '	sort_specification_list	goto 457',
    '	column_name	goto 458',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	unsigned_integer	goto 460',
    '	regular_identifier	goto 64',
    '',
    'state 286:',
    '',
    '	query_expression : query_expression _UNION all_opt _ corresponding_spec_opt query_term',
    '	corresponding_spec_opt : _	(358)',
    '',
    '	_CORRESPONDING	shift 453',
    '	left_paren	reduce 358',
    '	_SELECT	reduce 358',
    '	_TABLE	reduce 358',
    '	_VALUES	reduce 358',
    '	.	error',
    '',
    '	corresponding_spec	goto 451',
    '	corresponding_spec_opt	goto 461',
    '',
    'state 287:',
    '',
    '	module : module_name_clause language_clause module_authorization_clause _ module_opt',
    '',
    '	_DECLARE	shift 468',
    '	_PROCEDURE	shift 469',
    '	.	error',
    '',
    '	procedure	goto 462',
    '	dynamic_declare_cursor	goto 463',
    '	declare_cursor	goto 464',
    '	module_contents	goto 465',
    '	temporary_table_declaration	goto 466',
    '	module_opt	goto 467',
    '',
    'state 288:',
    '',
    '	module_authorization_clause : _AUTHORIZATION _ module_authorization_identifier',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	authorization_identifier	goto 470',
    '	module_authorization_identifier	goto 471',
    '	actual_identifier	goto 61',
    '	identifier	goto 472',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 289:',
    '',
    '	module_authorization_clause : _SCHEMA _ schema_name',
    '	module_authorization_clause : _SCHEMA _ schema_name _AUTHORIZATION module_authorization_identifier',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	actual_identifier	goto 61',
    '	schema_name	goto 473',
    '	identifier	goto 319',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 290:',
    '',
    '	language_clause : _LANGUAGE language_name _	(67)',
    '',
    '	.	reduce 67',
    '',
    'state 291:',
    '',
    '	language_name : _ADA _	(68)',
    '',
    '	.	reduce 68',
    '',
    'state 292:',
    '',
    '	language_name : _C _	(69)',
    '',
    '	.	reduce 69',
    '',
    'state 293:',
    '',
    '	language_name : _COBOL _	(70)',
    '',
    '	.	reduce 70',
    '',
    'state 294:',
    '',
    '	language_name : _FORTRAN _	(71)',
    '',
    '	.	reduce 71',
    '',
    'state 295:',
    '',
    '	language_name : _MUMPS _	(72)',
    '',
    '	.	reduce 72',
    '',
    'state 296:',
    '',
    '	language_name : _PASCAL _	(73)',
    '',
    '	.	reduce 73',
    '',
    'state 297:',
    '',
    '	language_name : _PLI _	(74)',
    '',
    '	.	reduce 74',
    '',
    'state 298:',
    '',
    '	character_set_name : identifier period _ identifier period SQL_language_identifier',
    '	character_set_name : identifier period _ SQL_language_identifier',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	actual_identifier	goto 61',
    '	SQL_language_identifier	goto 474',
    '	identifier	goto 475',
    '	introducer	goto 63',
    '	regular_identifier	goto 100',
    '',
    'state 299:',
    '',
    '	identifier : introducer character_set_specification actual_identifier _	(38)',
    '',
    '	.	reduce 38',
    '',
    'state 300:',
    '',
    '	table_subquery : left_paren query_expression right_paren _	(400)',
    '',
    '	.	reduce 400',
    '',
    'state 301:',
    '',
    '	domain_name : qualified_name _	(186)',
    '',
    '	.	reduce 186',
    '',
    'state 302:',
    '',
    '	alter_domain_statement : _ALTER _DOMAIN domain_name _ alter_domain_action',
    '',
    '	_ADD	shift 481',
    '	_DROP	shift 482',
    '	_SET	shift 483',
    '	.	error',
    '',
    '	drop_domain_constraint_definition	goto 476',
    '	add_domain_constraint_definition	goto 477',
    '	drop_domain_default_clause	goto 478',
    '	set_domain_default_clause	goto 479',
    '	alter_domain_action	goto 480',
    '',
    'state 303:',
    '',
    '	alter_table_statement : _ALTER _TABLE table_name _ alter_table_action',
    '',
    '	_ADD	shift 490',
    '	_ALTER	shift 491',
    '	_DROP	shift 492',
    '	.	error',
    '',
    '	drop_table_constraint_definition	goto 484',
    '	add_table_constraint_definition	goto 485',
    '	drop_column_definition	goto 486',
    '	alter_column_definition	goto 487',
    '	add_column_definition	goto 488',
    '	alter_table_action	goto 489',
    '',
    'state 304:',
    '',
    '	connection_target : SQL_server_name _ connection_name_opt user_name_opt',
    '	connection_name_opt : _	(858)',
    '',
    '	_AS	shift 494',
    '	$end	reduce 858',
    '	identifier_body	reduce 858',
    '	delimited_identifier	reduce 858',
    '	left_paren	reduce 858',
    '	semicolon	reduce 858',
    '	underscore	reduce 858',
    '	_ALTER	reduce 858',
    '	_COMMIT	reduce 858',
    '	_CONNECT	reduce 858',
    '	_CREATE	reduce 858',
    '	_DECLARE	reduce 858',
    '	_DELETE	reduce 858',
    '	_DISCONNECT	reduce 858',
    '	_DROP	reduce 858',
    '	_GRANT	reduce 858',
    '	_INSERT	reduce 858',
    '	_REVOKE	reduce 858',
    '	_ROLLBACK	reduce 858',
    '	_SELECT	reduce 858',
    '	_SET	reduce 858',
    '	_TABLE	reduce 858',
    '	_UPDATE	reduce 858',
    '	_USER	reduce 858',
    '	_VALUES	reduce 858',
    '	.	error',
    '',
    '	connection_name_opt	goto 493',
    '',
    'state 305:',
    '',
    '	connect_statement : _CONNECT _TO connection_target _	(855)',
    '',
    '	.	reduce 855',
    '',
    'state 306:',
    '',
    '	SQL_server_name : simple_value_specification _	(862)',
    '',
    '	.	reduce 862',
    '',
    'state 307:',
    '',
    '	connection_target : _DEFAULT _	(857)',
    '',
    '	.	reduce 857',
    '',
    'state 308:',
    '',
    '	table_definition : _CREATE table_definition_opts _TABLE _ table_name table_element_list table_commit_opts',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	table_name	goto 495',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 309:',
    '',
    '	assertion_definition : _CREATE _ASSERTION constraint_name _ assertion_check constraint_attributes_opt',
    '',
    '	_CHECK	shift 497',
    '	.	error',
    '',
    '	assertion_check	goto 496',
    '',
    'state 310:',
    '',
    '	constraint_name : qualified_name _	(231)',
    '',
    '	.	reduce 231',
    '',
    'state 311:',
    '',
    '	character_set_definition : _CREATE _CHARACTER _SET _ character_set_name as_opt character_set_source charset_collation_opt',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	actual_identifier	goto 61',
    '	SQL_language_identifier	goto 96',
    '	identifier	goto 97',
    '	character_set_name	goto 498',
    '	introducer	goto 63',
    '	regular_identifier	goto 100',
    '',
    'state 312:',
    '',
    '	collation_definition : _CREATE _COLLATION collation_name _ _FOR character_set_specification _FROM collation_source pad_attribute_opt',
    '',
    '	_FOR	shift 499',
    '	.	error',
    '',
    'state 313:',
    '',
    '	collation_name : qualified_name _	(429)',
    '',
    '	.	reduce 429',
    '',
    'state 314:',
    '',
    '	domain_definition : _CREATE _DOMAIN domain_name _ as_opt data_type default_clause_opt domain_constraint_opt collate_clause_opt',
    '	as_opt : _	(394)',
    '',
    '	_AS	shift 501',
    '	_BIT	reduce 394',
    '	_CHAR	reduce 394',
    '	_CHARACTER	reduce 394',
    '	_DATE	reduce 394',
    '	_DEC	reduce 394',
    '	_DECIMAL	reduce 394',
    '	_DOUBLE	reduce 394',
    '	_FLOAT	reduce 394',
    '	_INT	reduce 394',
    '	_INTEGER	reduce 394',
    '	_INTERVAL	reduce 394',
    '	_NATIONAL	reduce 394',
    '	_NCHAR	reduce 394',
    '	_NUMERIC	reduce 394',
    '	_REAL	reduce 394',
    '	_SMALLINT	reduce 394',
    '	_TIME	reduce 394',
    '	_TIMESTAMP	reduce 394',
    '	_VARCHAR	reduce 394',
    '	.	error',
    '',
    '	as_opt	goto 500',
    '',
    'state 315:',
    '',
    '	table_definition_opts : _GLOBAL _TEMPORARY _	(653)',
    '',
    '	.	reduce 653',
    '',
    'state 316:',
    '',
    '	table_definition_opts : _LOCAL _TEMPORARY _	(654)',
    '',
    '	.	reduce 654',
    '',
    'state 317:',
    '',
    '	schema_definition : _CREATE _SCHEMA schema_name_clause _ schema_character_set_specification_opt schema_elements',
    '	schema_character_set_specification_opt : _	(630)',
    '',
    '	_DEFAULT	shift 504',
    '	_CREATE	reduce 630',
    '	_GRANT	reduce 630',
    '	.	error',
    '',
    '	schema_character_set_specification	goto 502',
    '	schema_character_set_specification_opt	goto 503',
    '',
    'state 318:',
    '',
    '	schema_name_clause : schema_name _	(634)',
    '	schema_name_clause : schema_name _ _AUTHORIZATION schema_authorization_identifier',
    '',
    '	_AUTHORIZATION	shift 505',
    '	_CREATE	reduce 634',
    '	_DEFAULT	reduce 634',
    '	_GRANT	reduce 634',
    '	.	error',
    '',
    'state 319:',
    '',
    '	schema_name : identifier _ period identifier',
    '	schema_name : identifier _	(37)',
    '',
    '	period	shift 506',
    '	_AUTHORIZATION	reduce 37',
    '	_CASCADE	reduce 37',
    '	_CREATE	reduce 37',
    '	_DECLARE	reduce 37',
    '	_DEFAULT	reduce 37',
    '	_GRANT	reduce 37',
    '	_PROCEDURE	reduce 37',
    '	_RESTRICT	reduce 37',
    '	.	error',
    '',
    'state 320:',
    '',
    '	schema_name_clause : _AUTHORIZATION _ schema_authorization_identifier',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	schema_authorization_identifier	goto 507',
    '	authorization_identifier	goto 508',
    '	actual_identifier	goto 61',
    '	identifier	goto 472',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 321:',
    '',
    '	translation_definition : _CREATE _TRANSLATION translation_name _ _FOR source_character_set_specification _TO target_character_set_specification _FROM translation_source',
    '',
    '	_FOR	shift 509',
    '	.	error',
    '',
    'state 322:',
    '',
    '	translation_name : qualified_name _	(482)',
    '',
    '	.	reduce 482',
    '',
    'state 323:',
    '',
    '	view_definition : _CREATE _VIEW table_name _ view_column_list_opt _AS query_expression view_check_opt',
    '	view_column_list_opt : _	(659)',
    '',
    '	left_paren	shift 511',
    '	_AS	reduce 659',
    '	.	error',
    '',
    '	view_column_list_opt	goto 510',
    '',
    'state 324:',
    '',
    '	temporary_table_declaration : _DECLARE _LOCAL _TEMPORARY _ _TABLE qualified_local_table_name table_element_list temporary_table_declaration_opt',
    '',
    '	_TABLE	shift 512',
    '	.	error',
    '',
    'state 325:',
    '',
    '	delete_statement__searched : _DELETE _FROM table_name _ where_clause_opt',
    '	where_clause_opt : _	(377)',
    '',
    '	_WHERE	shift 515',
    '	$end	reduce 377',
    '	identifier_body	reduce 377',
    '	delimited_identifier	reduce 377',
    '	left_paren	reduce 377',
    '	underscore	reduce 377',
    '	_ALTER	reduce 377',
    '	_COMMIT	reduce 377',
    '	_CONNECT	reduce 377',
    '	_CREATE	reduce 377',
    '	_DECLARE	reduce 377',
    '	_DELETE	reduce 377',
    '	_DISCONNECT	reduce 377',
    '	_DROP	reduce 377',
    '	_GRANT	reduce 377',
    '	_INSERT	reduce 377',
    '	_REVOKE	reduce 377',
    '	_ROLLBACK	reduce 377',
    '	_SELECT	reduce 377',
    '	_SET	reduce 377',
    '	_TABLE	reduce 377',
    '	_UPDATE	reduce 377',
    '	_VALUES	reduce 377',
    '	.	error',
    '',
    '	where_clause	goto 513',
    '	where_clause_opt	goto 514',
    '',
    'state 326:',
    '',
    '	character_string_literal_main : character_string_literal_main string_literal_continuation _	(30)',
    '',
    '	.	reduce 30',
    '',
    'state 327:',
    '',
    '	character_string_literal : introducer character_set_specification _ character_string_literal_main',
    '',
    '	string_literal_continuation	shift 145',
    '	.	error',
    '',
    '	character_string_literal_main	goto 516',
    '',
    'state 328:',
    '',
    '	signed_numeric_literal : sign unsigned_numeric_literal _	(203)',
    '',
    '	.	reduce 203',
    '',
    'state 329:',
    '',
    '	approximate_numeric_literal : mantissa _E _ exponent',
    '',
    '	digit	shift 147',
    '	plus_sign	shift 148',
    '	minus_sign	shift 149',
    '	.	error',
    '',
    '	sign	goto 517',
    '	signed_integer	goto 518',
    '	exponent	goto 519',
    '	unsigned_integer	goto 520',
    '',
    'state 330:',
    '',
    '	exact_numeric_literal : unsigned_integer exact_numeric_literal_opt _	(4)',
    '',
    '	.	reduce 4',
    '',
    'state 331:',
    '',
    '	unsigned_integer : unsigned_integer digit _	(10)',
    '',
    '	.	reduce 10',
    '',
    'state 332:',
    '',
    '	exact_numeric_literal_opt : period _	(7)',
    '	exact_numeric_literal_opt : period _ unsigned_integer',
    '',
    '	digit	shift 147',
    '	$end	reduce 7',
    '	identifier_body	reduce 7',
    '	delimited_identifier	reduce 7',
    '	not_equals_operator	reduce 7',
    '	greater_than_or_equals_operator	reduce 7',
    '	less_than_or_equals_operator	reduce 7',
    '	concatenation_operator	reduce 7',
    '	left_paren	reduce 7',
    '	right_paren	reduce 7',
    '	asterisk	reduce 7',
    '	plus_sign	reduce 7',
    '	comma	reduce 7',
    '	minus_sign	reduce 7',
    '	solidus	reduce 7',
    '	semicolon	reduce 7',
    '	less_than_operator	reduce 7',
    '	equals_operator	reduce 7',
    '	greater_than_operator	reduce 7',
    '	underscore	reduce 7',
    '	_ALTER	reduce 7',
    '	_AND	reduce 7',
    '	_AS	reduce 7',
    '	_AT	reduce 7',
    '	_BETWEEN	reduce 7',
    '	_CHECK	reduce 7',
    '	_COLLATE	reduce 7',
    '	_COMMIT	reduce 7',
    '	_CONNECT	reduce 7',
    '	_CONSTRAINT	reduce 7',
    '	_CREATE	reduce 7',
    '	_CROSS	reduce 7',
    '	_DAY	reduce 7',
    '	_DECLARE	reduce 7',
    '	_DELETE	reduce 7',
    '	_DISCONNECT	reduce 7',
    '	_DROP	reduce 7',
    '	_ELSE	reduce 7',
    '	_END	reduce 7',
    '	_ESCAPE	reduce 7',
    '	_EXCEPT	reduce 7',
    '	_FOR	reduce 7',
    '	_FROM	reduce 7',
    '	_FULL	reduce 7',
    '	_GRANT	reduce 7',
    '	_GROUP	reduce 7',
    '	_HAVING	reduce 7',
    '	_HOUR	reduce 7',
    '	_IN	reduce 7',
    '	_INNER	reduce 7',
    '	_INSERT	reduce 7',
    '	_INTERSECT	reduce 7',
    '	_INTO	reduce 7',
    '	_IS	reduce 7',
    '	_JOIN	reduce 7',
    '	_LEFT	reduce 7',
    '	_LIKE	reduce 7',
    '	_MATCH	reduce 7',
    '	_MINUTE	reduce 7',
    '	_MONTH	reduce 7',
    '	_NATURAL	reduce 7',
    '	_NOT	reduce 7',
    '	_OR	reduce 7',
    '	_ORDER	reduce 7',
    '	_OVERLAPS	reduce 7',
    '	_PRIMARY	reduce 7',
    '	_REFERENCES	reduce 7',
    '	_REVOKE	reduce 7',
    '	_RIGHT	reduce 7',
    '	_ROLLBACK	reduce 7',
    '	_SECOND	reduce 7',
    '	_SELECT	reduce 7',
    '	_SET	reduce 7',
    '	_TABLE	reduce 7',
    '	_THEN	reduce 7',
    '	_UNION	reduce 7',
    '	_UNIQUE	reduce 7',
    '	_UPDATE	reduce 7',
    '	_USER	reduce 7',
    '	_USING	reduce 7',
    '	_VALUES	reduce 7',
    '	_WHEN	reduce 7',
    '	_WHERE	reduce 7',
    '	_WITH	reduce 7',
    '	_YEAR	reduce 7',
    '	_E	reduce 7',
    '	.	error',
    '',
    '	unsigned_integer	goto 521',
    '',
    'state 333:',
    '',
    '	national_character_string_literal : national_character_string_literal_start national_character_string_literal_cont _	(18)',
    '	national_character_string_literal_cont : national_character_string_literal_cont _ string_literal_continuation',
    '',
    '	string_literal_continuation	shift 522',
    '	$end	reduce 18',
    '	identifier_body	reduce 18',
    '	delimited_identifier	reduce 18',
    '	not_equals_operator	reduce 18',
    '	greater_than_or_equals_operator	reduce 18',
    '	less_than_or_equals_operator	reduce 18',
    '	concatenation_operator	reduce 18',
    '	left_paren	reduce 18',
    '	right_paren	reduce 18',
    '	asterisk	reduce 18',
    '	plus_sign	reduce 18',
    '	comma	reduce 18',
    '	minus_sign	reduce 18',
    '	solidus	reduce 18',
    '	semicolon	reduce 18',
    '	less_than_operator	reduce 18',
    '	equals_operator	reduce 18',
    '	greater_than_operator	reduce 18',
    '	underscore	reduce 18',
    '	_ALTER	reduce 18',
    '	_AND	reduce 18',
    '	_AS	reduce 18',
    '	_AT	reduce 18',
    '	_BETWEEN	reduce 18',
    '	_CHECK	reduce 18',
    '	_COLLATE	reduce 18',
    '	_COMMIT	reduce 18',
    '	_CONNECT	reduce 18',
    '	_CONSTRAINT	reduce 18',
    '	_CREATE	reduce 18',
    '	_CROSS	reduce 18',
    '	_DAY	reduce 18',
    '	_DECLARE	reduce 18',
    '	_DELETE	reduce 18',
    '	_DISCONNECT	reduce 18',
    '	_DROP	reduce 18',
    '	_ELSE	reduce 18',
    '	_END	reduce 18',
    '	_ESCAPE	reduce 18',
    '	_EXCEPT	reduce 18',
    '	_FOR	reduce 18',
    '	_FROM	reduce 18',
    '	_FULL	reduce 18',
    '	_GRANT	reduce 18',
    '	_GROUP	reduce 18',
    '	_HAVING	reduce 18',
    '	_HOUR	reduce 18',
    '	_IN	reduce 18',
    '	_INNER	reduce 18',
    '	_INSERT	reduce 18',
    '	_INTERSECT	reduce 18',
    '	_INTO	reduce 18',
    '	_IS	reduce 18',
    '	_JOIN	reduce 18',
    '	_LEFT	reduce 18',
    '	_LIKE	reduce 18',
    '	_MATCH	reduce 18',
    '	_MINUTE	reduce 18',
    '	_MONTH	reduce 18',
    '	_NATURAL	reduce 18',
    '	_NOT	reduce 18',
    '	_OR	reduce 18',
    '	_ORDER	reduce 18',
    '	_OVERLAPS	reduce 18',
    '	_PRIMARY	reduce 18',
    '	_REFERENCES	reduce 18',
    '	_REVOKE	reduce 18',
    '	_RIGHT	reduce 18',
    '	_ROLLBACK	reduce 18',
    '	_SECOND	reduce 18',
    '	_SELECT	reduce 18',
    '	_SET	reduce 18',
    '	_TABLE	reduce 18',
    '	_THEN	reduce 18',
    '	_UNION	reduce 18',
    '	_UNIQUE	reduce 18',
    '	_UPDATE	reduce 18',
    '	_USER	reduce 18',
    '	_USING	reduce 18',
    '	_VALUES	reduce 18',
    '	_WHEN	reduce 18',
    '	_WHERE	reduce 18',
    '	_WITH	reduce 18',
    '	_YEAR	reduce 18',
    '	.	error',
    '',
    'state 334:',
    '',
    '	bit_string_literal : bit_string_literal_start bit_string_literal_cont _	(21)',
    '	bit_string_literal_cont : bit_string_literal_cont _ string_literal_continuation',
    '',
    '	string_literal_continuation	shift 523',
    '	$end	reduce 21',
    '	identifier_body	reduce 21',
    '	delimited_identifier	reduce 21',
    '	not_equals_operator	reduce 21',
    '	greater_than_or_equals_operator	reduce 21',
    '	less_than_or_equals_operator	reduce 21',
    '	concatenation_operator	reduce 21',
    '	left_paren	reduce 21',
    '	right_paren	reduce 21',
    '	asterisk	reduce 21',
    '	plus_sign	reduce 21',
    '	comma	reduce 21',
    '	minus_sign	reduce 21',
    '	solidus	reduce 21',
    '	semicolon	reduce 21',
    '	less_than_operator	reduce 21',
    '	equals_operator	reduce 21',
    '	greater_than_operator	reduce 21',
    '	underscore	reduce 21',
    '	_ALTER	reduce 21',
    '	_AND	reduce 21',
    '	_AS	reduce 21',
    '	_AT	reduce 21',
    '	_BETWEEN	reduce 21',
    '	_CHECK	reduce 21',
    '	_COLLATE	reduce 21',
    '	_COMMIT	reduce 21',
    '	_CONNECT	reduce 21',
    '	_CONSTRAINT	reduce 21',
    '	_CREATE	reduce 21',
    '	_CROSS	reduce 21',
    '	_DAY	reduce 21',
    '	_DECLARE	reduce 21',
    '	_DELETE	reduce 21',
    '	_DISCONNECT	reduce 21',
    '	_DROP	reduce 21',
    '	_ELSE	reduce 21',
    '	_END	reduce 21',
    '	_ESCAPE	reduce 21',
    '	_EXCEPT	reduce 21',
    '	_FOR	reduce 21',
    '	_FROM	reduce 21',
    '	_FULL	reduce 21',
    '	_GRANT	reduce 21',
    '	_GROUP	reduce 21',
    '	_HAVING	reduce 21',
    '	_HOUR	reduce 21',
    '	_IN	reduce 21',
    '	_INNER	reduce 21',
    '	_INSERT	reduce 21',
    '	_INTERSECT	reduce 21',
    '	_INTO	reduce 21',
    '	_IS	reduce 21',
    '	_JOIN	reduce 21',
    '	_LEFT	reduce 21',
    '	_LIKE	reduce 21',
    '	_MATCH	reduce 21',
    '	_MINUTE	reduce 21',
    '	_MONTH	reduce 21',
    '	_NATURAL	reduce 21',
    '	_NOT	reduce 21',
    '	_OR	reduce 21',
    '	_ORDER	reduce 21',
    '	_OVERLAPS	reduce 21',
    '	_PRIMARY	reduce 21',
    '	_REFERENCES	reduce 21',
    '	_REVOKE	reduce 21',
    '	_RIGHT	reduce 21',
    '	_ROLLBACK	reduce 21',
    '	_SECOND	reduce 21',
    '	_SELECT	reduce 21',
    '	_SET	reduce 21',
    '	_TABLE	reduce 21',
    '	_THEN	reduce 21',
    '	_UNION	reduce 21',
    '	_UNIQUE	reduce 21',
    '	_UPDATE	reduce 21',
    '	_USER	reduce 21',
    '	_USING	reduce 21',
    '	_VALUES	reduce 21',
    '	_WHEN	reduce 21',
    '	_WHERE	reduce 21',
    '	_WITH	reduce 21',
    '	_YEAR	reduce 21',
    '	.	error',
    '',
    'state 335:',
    '',
    '	hex_string_literal : hex_string_literal_start hex_string_literal_cont _	(24)',
    '	hex_string_literal_cont : hex_string_literal_cont _ string_literal_continuation',
    '',
    '	string_literal_continuation	shift 524',
    '	$end	reduce 24',
    '	identifier_body	reduce 24',
    '	delimited_identifier	reduce 24',
    '	not_equals_operator	reduce 24',
    '	greater_than_or_equals_operator	reduce 24',
    '	less_than_or_equals_operator	reduce 24',
    '	concatenation_operator	reduce 24',
    '	left_paren	reduce 24',
    '	right_paren	reduce 24',
    '	asterisk	reduce 24',
    '	plus_sign	reduce 24',
    '	comma	reduce 24',
    '	minus_sign	reduce 24',
    '	solidus	reduce 24',
    '	semicolon	reduce 24',
    '	less_than_operator	reduce 24',
    '	equals_operator	reduce 24',
    '	greater_than_operator	reduce 24',
    '	underscore	reduce 24',
    '	_ALTER	reduce 24',
    '	_AND	reduce 24',
    '	_AS	reduce 24',
    '	_AT	reduce 24',
    '	_BETWEEN	reduce 24',
    '	_CHECK	reduce 24',
    '	_COLLATE	reduce 24',
    '	_COMMIT	reduce 24',
    '	_CONNECT	reduce 24',
    '	_CONSTRAINT	reduce 24',
    '	_CREATE	reduce 24',
    '	_CROSS	reduce 24',
    '	_DAY	reduce 24',
    '	_DECLARE	reduce 24',
    '	_DELETE	reduce 24',
    '	_DISCONNECT	reduce 24',
    '	_DROP	reduce 24',
    '	_ELSE	reduce 24',
    '	_END	reduce 24',
    '	_ESCAPE	reduce 24',
    '	_EXCEPT	reduce 24',
    '	_FOR	reduce 24',
    '	_FROM	reduce 24',
    '	_FULL	reduce 24',
    '	_GRANT	reduce 24',
    '	_GROUP	reduce 24',
    '	_HAVING	reduce 24',
    '	_HOUR	reduce 24',
    '	_IN	reduce 24',
    '	_INNER	reduce 24',
    '	_INSERT	reduce 24',
    '	_INTERSECT	reduce 24',
    '	_INTO	reduce 24',
    '	_IS	reduce 24',
    '	_JOIN	reduce 24',
    '	_LEFT	reduce 24',
    '	_LIKE	reduce 24',
    '	_MATCH	reduce 24',
    '	_MINUTE	reduce 24',
    '	_MONTH	reduce 24',
    '	_NATURAL	reduce 24',
    '	_NOT	reduce 24',
    '	_OR	reduce 24',
    '	_ORDER	reduce 24',
    '	_OVERLAPS	reduce 24',
    '	_PRIMARY	reduce 24',
    '	_REFERENCES	reduce 24',
    '	_REVOKE	reduce 24',
    '	_RIGHT	reduce 24',
    '	_ROLLBACK	reduce 24',
    '	_SECOND	reduce 24',
    '	_SELECT	reduce 24',
    '	_SET	reduce 24',
    '	_TABLE	reduce 24',
    '	_THEN	reduce 24',
    '	_UNION	reduce 24',
    '	_UNIQUE	reduce 24',
    '	_UPDATE	reduce 24',
    '	_USER	reduce 24',
    '	_USING	reduce 24',
    '	_VALUES	reduce 24',
    '	_WHEN	reduce 24',
    '	_WHERE	reduce 24',
    '	_WITH	reduce 24',
    '	_YEAR	reduce 24',
    '	.	error',
    '',
    'state 336:',
    '',
    '	exact_numeric_literal : period unsigned_integer _	(5)',
    '	unsigned_integer : unsigned_integer _ digit',
    '',
    '	digit	shift 331',
    '	$end	reduce 5',
    '	identifier_body	reduce 5',
    '	delimited_identifier	reduce 5',
    '	not_equals_operator	reduce 5',
    '	greater_than_or_equals_operator	reduce 5',
    '	less_than_or_equals_operator	reduce 5',
    '	concatenation_operator	reduce 5',
    '	left_paren	reduce 5',
    '	right_paren	reduce 5',
    '	asterisk	reduce 5',
    '	plus_sign	reduce 5',
    '	comma	reduce 5',
    '	minus_sign	reduce 5',
    '	solidus	reduce 5',
    '	semicolon	reduce 5',
    '	less_than_operator	reduce 5',
    '	equals_operator	reduce 5',
    '	greater_than_operator	reduce 5',
    '	underscore	reduce 5',
    '	_ALTER	reduce 5',
    '	_AND	reduce 5',
    '	_AS	reduce 5',
    '	_AT	reduce 5',
    '	_BETWEEN	reduce 5',
    '	_CHECK	reduce 5',
    '	_COLLATE	reduce 5',
    '	_COMMIT	reduce 5',
    '	_CONNECT	reduce 5',
    '	_CONSTRAINT	reduce 5',
    '	_CREATE	reduce 5',
    '	_CROSS	reduce 5',
    '	_DAY	reduce 5',
    '	_DECLARE	reduce 5',
    '	_DELETE	reduce 5',
    '	_DISCONNECT	reduce 5',
    '	_DROP	reduce 5',
    '	_ELSE	reduce 5',
    '	_END	reduce 5',
    '	_ESCAPE	reduce 5',
    '	_EXCEPT	reduce 5',
    '	_FOR	reduce 5',
    '	_FROM	reduce 5',
    '	_FULL	reduce 5',
    '	_GRANT	reduce 5',
    '	_GROUP	reduce 5',
    '	_HAVING	reduce 5',
    '	_HOUR	reduce 5',
    '	_IN	reduce 5',
    '	_INNER	reduce 5',
    '	_INSERT	reduce 5',
    '	_INTERSECT	reduce 5',
    '	_INTO	reduce 5',
    '	_IS	reduce 5',
    '	_JOIN	reduce 5',
    '	_LEFT	reduce 5',
    '	_LIKE	reduce 5',
    '	_MATCH	reduce 5',
    '	_MINUTE	reduce 5',
    '	_MONTH	reduce 5',
    '	_NATURAL	reduce 5',
    '	_NOT	reduce 5',
    '	_OR	reduce 5',
    '	_ORDER	reduce 5',
    '	_OVERLAPS	reduce 5',
    '	_PRIMARY	reduce 5',
    '	_REFERENCES	reduce 5',
    '	_REVOKE	reduce 5',
    '	_RIGHT	reduce 5',
    '	_ROLLBACK	reduce 5',
    '	_SECOND	reduce 5',
    '	_SELECT	reduce 5',
    '	_SET	reduce 5',
    '	_TABLE	reduce 5',
    '	_THEN	reduce 5',
    '	_UNION	reduce 5',
    '	_UNIQUE	reduce 5',
    '	_UPDATE	reduce 5',
    '	_USER	reduce 5',
    '	_USING	reduce 5',
    '	_VALUES	reduce 5',
    '	_WHEN	reduce 5',
    '	_WHERE	reduce 5',
    '	_WITH	reduce 5',
    '	_YEAR	reduce 5',
    '	_E	reduce 5',
    '	.	error',
    '',
    'state 337:',
    '',
    '	parameter_name : colon identifier _	(331)',
    '',
    '	.	reduce 331',
    '',
    'state 338:',
    '',
    '	date_literal : _DATE date_string _	(214)',
    '',
    '	.	reduce 214',
    '',
    'state 339:',
    '',
    '	date_string : quote _ date_value quote',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	date_value	goto 525',
    '	unsigned_integer	goto 526',
    '',
    'state 340:',
    '',
    '	interval_literal : _INTERVAL interval_string _ interval_qualifier',
    '',
    '	_DAY	shift 415',
    '	_HOUR	shift 416',
    '	_MINUTE	shift 417',
    '	_MONTH	shift 418',
    '	_SECOND	shift 419',
    '	_YEAR	shift 420',
    '	.	error',
    '',
    '	non_second_datetime_field	goto 409',
    '	start_field	goto 410',
    '	interval_qualifier	goto 527',
    '',
    'state 341:',
    '',
    '	interval_literal : _INTERVAL sign _ interval_string interval_qualifier',
    '',
    '	quote	shift 342',
    '	.	error',
    '',
    '	interval_string	goto 528',
    '',
    'state 342:',
    '',
    '	interval_string : quote _ interval_string_literal quote',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	interval_string_literal	goto 529',
    '	unsigned_integer	goto 530',
    '',
    'state 343:',
    '',
    '	time_literal : _TIME time_string _	(215)',
    '',
    '	.	reduce 215',
    '',
    'state 344:',
    '',
    '	time_string : quote _ time_value quote quote time_value time_zone_interval quote',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	time_value	goto 531',
    '	unsigned_integer	goto 532',
    '',
    'state 345:',
    '',
    '	timestamp_literal : _TIMESTAMP timestamp_string _	(216)',
    '',
    '	.	reduce 216',
    '',
    'state 346:',
    '',
    '	timestamp_string : quote _ date_value space time_value quote',
    '	timestamp_string : quote _ date_value space time_value time_zone_interval quote',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	date_value	goto 533',
    '	unsigned_integer	goto 526',
    '',
    'state 347:',
    '',
    '	drop_assertion_statement : _DROP _ASSERTION constraint_name _	(777)',
    '',
    '	.	reduce 777',
    '',
    'state 348:',
    '',
    '	drop_character_set_statement : _DROP _CHARACTER _SET _ character_set_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	actual_identifier	goto 61',
    '	SQL_language_identifier	goto 96',
    '	identifier	goto 97',
    '	character_set_name	goto 534',
    '	introducer	goto 63',
    '	regular_identifier	goto 100',
    '',
    'state 349:',
    '',
    '	drop_collation_statement : _DROP _COLLATION collation_name _	(775)',
    '',
    '	.	reduce 775',
    '',
    'state 350:',
    '',
    '	drop_domain_statement : _DROP _DOMAIN domain_name _ drop_behaviour',
    '',
    '	_CASCADE	shift 536',
    '	_RESTRICT	shift 537',
    '	.	error',
    '',
    '	drop_behaviour	goto 535',
    '',
    'state 351:',
    '',
    '	drop_schema_statement : _DROP _SCHEMA schema_name _ drop_behaviour',
    '',
    '	_CASCADE	shift 536',
    '	_RESTRICT	shift 537',
    '	.	error',
    '',
    '	drop_behaviour	goto 538',
    '',
    'state 352:',
    '',
    '	drop_table_statement : _DROP _TABLE table_name _ drop_behaviour',
    '',
    '	_CASCADE	shift 536',
    '	_RESTRICT	shift 537',
    '	.	error',
    '',
    '	drop_behaviour	goto 539',
    '',
    'state 353:',
    '',
    '	drop_translation_statement : _DROP _TRANSLATION translation_name _	(776)',
    '',
    '	.	reduce 776',
    '',
    'state 354:',
    '',
    '	drop_view_statement : _DROP _VIEW table_name _ drop_behaviour',
    '',
    '	_CASCADE	shift 536',
    '	_RESTRICT	shift 537',
    '	.	error',
    '',
    '	drop_behaviour	goto 540',
    '',
    'state 355:',
    '',
    '	action_list : action_list comma _ action',
    '',
    '	_DELETE	shift 171',
    '	_INSERT	shift 172',
    '	_REFERENCES	shift 173',
    '	_SELECT	shift 174',
    '	_UPDATE	shift 175',
    '	_USAGE	shift 176',
    '	.	error',
    '',
    '	action	goto 541',
    '',
    'state 356:',
    '',
    '	grant_statement : _GRANT privileges _ON _ object_name _TO grantee_list grant_option',
    '	table_opt : _	(689)',
    '',
    '	_CHARACTER	shift 544',
    '	_COLLATION	shift 545',
    '	_DOMAIN	shift 546',
    '	_TABLE	shift 547',
    '	_TRANSLATION	shift 548',
    '	identifier_body	reduce 689',
    '	delimited_identifier	reduce 689',
    '	underscore	reduce 689',
    '	_MODULE	reduce 689',
    '	.	error',
    '',
    '	table_opt	goto 542',
    '	object_name	goto 543',
    '',
    'state 357:',
    '',
    '	privileges : _ALL _PRIVILEGES _	(671)',
    '',
    '	.	reduce 671',
    '',
    'state 358:',
    '',
    '	action : _INSERT privilege_column_list_opt _	(677)',
    '',
    '	.	reduce 677',
    '',
    'state 359:',
    '',
    '	privilege_column_list_opt : left_paren _ privilege_column_list right_paren',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	privilege_column_list	goto 549',
    '	column_name_list	goto 550',
    '	column_name	goto 551',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 360:',
    '',
    '	action : _REFERENCES privilege_column_list_opt _	(679)',
    '',
    '	.	reduce 679',
    '',
    'state 361:',
    '',
    '	action : _UPDATE privilege_column_list_opt _	(678)',
    '',
    '	.	reduce 678',
    '',
    'state 362:',
    '',
    '	insert_statement : _INSERT _INTO table_name _ insert_columns_and_source',
    '',
    '	left_paren	shift 554',
    '	_DEFAULT	shift 555',
    '	_SELECT	shift 83',
    '	_TABLE	shift 85',
    '	_VALUES	shift 87',
    '	.	error',
    '',
    '	insert_columns_and_source	goto 552',
    '	explicit_table	goto 49',
    '	table_value_constructor	goto 50',
    '	query_specification	goto 51',
    '	table_subquery	goto 52',
    '	simple_table	goto 53',
    '	non_join_query_primary	goto 54',
    '	query_term	goto 55',
    '	non_join_query_term	goto 56',
    '	query_expression	goto 553',
    '',
    'state 363:',
    '',
    '	module_name_clause : _MODULE _MODULE module_name _ _MODULE module_character_set_specification _MODULE module_name module_character_set_specification',
    '',
    '	_MODULE	shift 556',
    '	.	error',
    '',
    'state 364:',
    '',
    '	module_name : identifier _	(65)',
    '',
    '	.	reduce 65',
    '',
    'state 365:',
    '',
    '	revoke_statement : _REVOKE grant_option_for_opt privileges _ _ON object_name _FROM grantee_list drop_behaviour',
    '',
    '	_ON	shift 557',
    '	.	error',
    '',
    'state 366:',
    '',
    '	grant_option_for_opt : _GRANT _OPTION _ _FOR',
    '',
    '	_FOR	shift 558',
    '	.	error',
    '',
    'state 367:',
    '',
    '	select_sublist : derived_column _	(370)',
    '',
    '	.	reduce 370',
    '',
    'state 368:',
    '',
    '	select_list_opt : select_sublist _	(368)',
    '',
    '	.	reduce 368',
    '',
    'state 369:',
    '',
    '	select_list : select_list_opt _	(367)',
    '	select_list_opt : select_list_opt _ comma select_sublist',
    '',
    '	comma	shift 559',
    '	_FROM	reduce 367',
    '	_INTO	reduce 367',
    '	.	error',
    '',
    'state 370:',
    '',
    '	query_specification : _SELECT set_quantifier_opt select_list _ table_expression',
    '',
    '	_FROM	shift 562',
    '	.	error',
    '',
    '	from_clause	goto 560',
    '	table_expression	goto 561',
    '',
    'state 371:',
    '',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	derived_column : expression _	(372)',
    '	derived_column : expression _ as_clause',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	underscore	shift 69',
    '	_AS	shift 565',
    '	comma	reduce 372',
    '	_FROM	reduce 372',
    '	_INTO	reduce 372',
    '	.	error',
    '',
    '	as_clause	goto 563',
    '	column_name	goto 564',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 372:',
    '',
    '	select_sublist : qualified_name_trail_asterisk _	(371)',
    '',
    '	.	reduce 371',
    '',
    'state 373:',
    '',
    '	qualified_name : identifier _	(187)',
    '	qualified_name : identifier _ period identifier',
    '	qualified_name : identifier _ period identifier period identifier',
    '	qualified_name_trail_asterisk : identifier _ period asterisk',
    '	qualified_name_trail_asterisk : identifier _ period identifier period asterisk',
    '	qualified_name_trail_asterisk : identifier _ period identifier period identifier period asterisk',
    '',
    '	period	shift 566',
    '	identifier_body	reduce 187',
    '	delimited_identifier	reduce 187',
    '	concatenation_operator	reduce 187',
    '	asterisk	reduce 187',
    '	plus_sign	reduce 187',
    '	comma	reduce 187',
    '	minus_sign	reduce 187',
    '	solidus	reduce 187',
    '	underscore	reduce 187',
    '	_AS	reduce 187',
    '	_AT	reduce 187',
    '	_COLLATE	reduce 187',
    '	_DAY	reduce 187',
    '	_FROM	reduce 187',
    '	_HOUR	reduce 187',
    '	_INTO	reduce 187',
    '	_MINUTE	reduce 187',
    '	_MONTH	reduce 187',
    '	_SECOND	reduce 187',
    '	_YEAR	reduce 187',
    '	.	error',
    '',
    'state 374:',
    '',
    '	primary_expression : left_paren _ expression right_paren',
    '	scalar_subquery : left_paren _ subquery right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 429',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SELECT	shift 83',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TABLE	shift 85',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_VALUES	shift 87',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	explicit_table	goto 49',
    '	table_value_constructor	goto 50',
    '	query_specification	goto 51',
    '	table_subquery	goto 52',
    '	simple_table	goto 53',
    '	non_join_query_primary	goto 54',
    '	query_term	goto 55',
    '	non_join_query_term	goto 56',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	query_expression	goto 425',
    '	subquery	goto 426',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 567',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 375:',
    '',
    '	select_list : asterisk _	(366)',
    '',
    '	.	reduce 366',
    '',
    'state 376:',
    '',
    '	set_catalog_statement : _SET _CATALOG value_specification _	(877)',
    '',
    '	.	reduce 877',
    '',
    'state 377:',
    '',
    '	value_specification : general_value_specification _	(879)',
    '',
    '	.	reduce 879',
    '',
    'state 378:',
    '',
    '	value_specification : literal _	(878)',
    '',
    '	.	reduce 878',
    '',
    'state 379:',
    '',
    '	set_connection_statement : _SET _CONNECTION connection_object _	(865)',
    '',
    '	.	reduce 865',
    '',
    'state 380:',
    '',
    '	constraint_name_list : constraint_name_list_some _	(845)',
    '	constraint_name_list_some : constraint_name_list_some _ comma constraint_name',
    '',
    '	comma	shift 568',
    '	_DEFERRED	reduce 845',
    '	_IMMEDIATE	reduce 845',
    '	.	error',
    '',
    'state 381:',
    '',
    '	set_constraints_mode_statement : _SET _CONSTRAINTS constraint_name_list _ _DEFERRED',
    '	set_constraints_mode_statement : _SET _CONSTRAINTS constraint_name_list _ _IMMEDIATE',
    '',
    '	_DEFERRED	shift 569',
    '	_IMMEDIATE	shift 570',
    '	.	error',
    '',
    'state 382:',
    '',
    '	constraint_name_list_some : constraint_name _	(846)',
    '',
    '	.	reduce 846',
    '',
    'state 383:',
    '',
    '	constraint_name_list : _ALL _	(844)',
    '',
    '	.	reduce 844',
    '',
    'state 384:',
    '',
    '	set_names_statement : _SET _NAMES value_specification _	(881)',
    '',
    '	.	reduce 881',
    '',
    'state 385:',
    '',
    '	set_schema_statement : _SET _SCHEMA value_specification _	(880)',
    '',
    '	.	reduce 880',
    '',
    'state 386:',
    '',
    '	set_session_authorization_identifier_statement : _SET _SESSION _AUTHORIZATION _ value_specification',
    '',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	digit	shift 147',
    '	plus_sign	shift 148',
    '	minus_sign	shift 149',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_INTERVAL	shift 156',
    '	_SESSION_USER	shift 272',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	.	error',
    '',
    '	value_specification	goto 571',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 377',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 128',
    '	signed_numeric_literal	goto 129',
    '	literal	goto 378',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 132',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	sign	goto 137',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 142',
    '',
    'state 387:',
    '',
    '	set_local_time_zone_statement : _SET _TIME _ZONE _ set_time_zone_value',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOCAL	shift 574',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	set_time_zone_value	goto 572',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 573',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 388:',
    '',
    '	transaction_mode : diagnostics_size _	(831)',
    '',
    '	.	reduce 831',
    '',
    'state 389:',
    '',
    '	transaction_mode : transaction_access_mode _	(830)',
    '',
    '	.	reduce 830',
    '',
    'state 390:',
    '',
    '	transaction_mode : isolation_level _	(829)',
    '',
    '	.	reduce 829',
    '',
    'state 391:',
    '',
    '	transaction_mode_list : transaction_mode _	(827)',
    '',
    '	.	reduce 827',
    '',
    'state 392:',
    '',
    '	set_transaction_statement : _SET _TRANSACTION transaction_mode_list _	(826)',
    '	transaction_mode_list : transaction_mode_list _ comma transaction_mode',
    '',
    '	comma	shift 575',
    '	$end	reduce 826',
    '	identifier_body	reduce 826',
    '	delimited_identifier	reduce 826',
    '	left_paren	reduce 826',
    '	semicolon	reduce 826',
    '	underscore	reduce 826',
    '	_ALTER	reduce 826',
    '	_COMMIT	reduce 826',
    '	_CONNECT	reduce 826',
    '	_CREATE	reduce 826',
    '	_DECLARE	reduce 826',
    '	_DELETE	reduce 826',
    '	_DISCONNECT	reduce 826',
    '	_DROP	reduce 826',
    '	_GRANT	reduce 826',
    '	_INSERT	reduce 826',
    '	_REVOKE	reduce 826',
    '	_ROLLBACK	reduce 826',
    '	_SELECT	reduce 826',
    '	_SET	reduce 826',
    '	_TABLE	reduce 826',
    '	_UPDATE	reduce 826',
    '	_VALUES	reduce 826',
    '	.	error',
    '',
    'state 393:',
    '',
    '	diagnostics_size : _DIAGNOSTICS _ _SIZE number_of_conditions',
    '',
    '	_SIZE	shift 576',
    '	.	error',
    '',
    'state 394:',
    '',
    '	isolation_level : _ISOLATION _ _LEVEL level_of_isolation',
    '',
    '	_LEVEL	shift 577',
    '	.	error',
    '',
    'state 395:',
    '',
    '	transaction_access_mode : _READ _ _ONLY',
    '	transaction_access_mode : _READ _ _WRITE',
    '',
    '	_ONLY	shift 578',
    '	_WRITE	shift 579',
    '	.	error',
    '',
    'state 396:',
    '',
    '	qualified_name : identifier period _ identifier',
    '	qualified_name : identifier period _ identifier period identifier',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	actual_identifier	goto 61',
    '	identifier	goto 580',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 397:',
    '',
    '	qualified_local_table_name : _MODULE period _ local_table_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	local_table_name	goto 581',
    '	actual_identifier	goto 61',
    '	identifier	goto 582',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 398:',
    '',
    '	update_statement__searched : _UPDATE table_name _SET _ set_clause_list where_clause_opt',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	object_column	goto 583',
    '	set_clause	goto 584',
    '	set_clause_list	goto 585',
    '	column_name	goto 586',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 399:',
    '',
    '	char_length_expression : char_length_specifier left_paren _ expression right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 587',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 400:',
    '',
    '	table_value_constructor_list : table_value_constructor_list comma _ row_value_constructor',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 248',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 238',
    '	row_value_constructor	goto 588',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 401:',
    '',
    '	general_set_function : set_function_type left_paren _ set_quantifier_args right_paren',
    '	set_quantifier_args : _	(339)',
    '',
    '	asterisk	shift 591',
    '	_ALL	shift 184',
    '	_DISTINCT	shift 185',
    '	right_paren	reduce 339',
    '	.	error',
    '',
    '	set_quantifier	goto 589',
    '	set_quantifier_args	goto 590',
    '',
    'state 402:',
    '',
    '	parameter_specification : parameter_name indicator_parameter_opt _	(330)',
    '',
    '	.	reduce 330',
    '',
    'state 403:',
    '',
    '	indicator_parameter_opt : parameter_name _	(334)',
    '',
    '	.	reduce 334',
    '',
    'state 404:',
    '',
    '	indicator_parameter_opt : _INDICATOR _ parameter_name',
    '',
    '	colon	shift 151',
    '	.	error',
    '',
    '	parameter_name	goto 592',
    '',
    'state 405:',
    '',
    '	multiplicative_expression : multiplicative_expression asterisk _ unary_expression',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	unary_expression	goto 593',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 406:',
    '',
    '	multiplicative_expression : multiplicative_expression solidus _ unary_expression',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	unary_expression	goto 594',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 407:',
    '',
    '	postfix_op : time_zone _	(304)',
    '',
    '	.	reduce 304',
    '',
    'state 408:',
    '',
    '	postfix_expression : primary_expression postfix_op _	(303)',
    '',
    '	.	reduce 303',
    '',
    'state 409:',
    '',
    '	*** conflicts:',
    '',
    '	shift 595, reduce 170 on left_paren',
    '',
    '	start_field : non_second_datetime_field _	(170)',
    '	start_field : non_second_datetime_field _ left_paren precision right_paren',
    '',
    '	left_paren	shift 595',
    '	$end	reduce 170',
    '	identifier_body	reduce 170',
    '	delimited_identifier	reduce 170',
    '	not_equals_operator	reduce 170',
    '	greater_than_or_equals_operator	reduce 170',
    '	less_than_or_equals_operator	reduce 170',
    '	concatenation_operator	reduce 170',
    '	right_paren	reduce 170',
    '	asterisk	reduce 170',
    '	plus_sign	reduce 170',
    '	comma	reduce 170',
    '	minus_sign	reduce 170',
    '	solidus	reduce 170',
    '	semicolon	reduce 170',
    '	less_than_operator	reduce 170',
    '	equals_operator	reduce 170',
    '	greater_than_operator	reduce 170',
    '	underscore	reduce 170',
    '	_ALTER	reduce 170',
    '	_AND	reduce 170',
    '	_AS	reduce 170',
    '	_AT	reduce 170',
    '	_BETWEEN	reduce 170',
    '	_CHECK	reduce 170',
    '	_COLLATE	reduce 170',
    '	_COMMIT	reduce 170',
    '	_CONNECT	reduce 170',
    '	_CONSTRAINT	reduce 170',
    '	_CREATE	reduce 170',
    '	_CROSS	reduce 170',
    '	_DAY	reduce 170',
    '	_DECLARE	reduce 170',
    '	_DEFAULT	reduce 170',
    '	_DELETE	reduce 170',
    '	_DISCONNECT	reduce 170',
    '	_DROP	reduce 170',
    '	_ELSE	reduce 170',
    '	_END	reduce 170',
    '	_ESCAPE	reduce 170',
    '	_EXCEPT	reduce 170',
    '	_FOR	reduce 170',
    '	_FROM	reduce 170',
    '	_FULL	reduce 170',
    '	_GRANT	reduce 170',
    '	_GROUP	reduce 170',
    '	_HAVING	reduce 170',
    '	_HOUR	reduce 170',
    '	_IN	reduce 170',
    '	_INNER	reduce 170',
    '	_INSERT	reduce 170',
    '	_INTERSECT	reduce 170',
    '	_INTO	reduce 170',
    '	_IS	reduce 170',
    '	_JOIN	reduce 170',
    '	_LEFT	reduce 170',
    '	_LIKE	reduce 170',
    '	_MATCH	reduce 170',
    '	_MINUTE	reduce 170',
    '	_MONTH	reduce 170',
    '	_NATURAL	reduce 170',
    '	_NOT	reduce 170',
    '	_OR	reduce 170',
    '	_ORDER	reduce 170',
    '	_OVERLAPS	reduce 170',
    '	_PRIMARY	reduce 170',
    '	_REFERENCES	reduce 170',
    '	_REVOKE	reduce 170',
    '	_RIGHT	reduce 170',
    '	_ROLLBACK	reduce 170',
    '	_SECOND	reduce 170',
    '	_SELECT	reduce 170',
    '	_SET	reduce 170',
    '	_TABLE	reduce 170',
    '	_THEN	reduce 170',
    '	_TO	reduce 170',
    '	_UNION	reduce 170',
    '	_UNIQUE	reduce 170',
    '	_UPDATE	reduce 170',
    '	_USER	reduce 170',
    '	_USING	reduce 170',
    '	_VALUES	reduce 170',
    '	_WHEN	reduce 170',
    '	_WHERE	reduce 170',
    '	_WITH	reduce 170',
    '	_YEAR	reduce 170',
    '	.	error',
    '',
    'state 410:',
    '',
    '	interval_qualifier : start_field _	(167)',
    '	interval_qualifier : start_field _ _TO end_field',
    '',
    '	_TO	shift 596',
    '	$end	reduce 167',
    '	identifier_body	reduce 167',
    '	delimited_identifier	reduce 167',
    '	not_equals_operator	reduce 167',
    '	greater_than_or_equals_operator	reduce 167',
    '	less_than_or_equals_operator	reduce 167',
    '	concatenation_operator	reduce 167',
    '	left_paren	reduce 167',
    '	right_paren	reduce 167',
    '	asterisk	reduce 167',
    '	plus_sign	reduce 167',
    '	comma	reduce 167',
    '	minus_sign	reduce 167',
    '	solidus	reduce 167',
    '	semicolon	reduce 167',
    '	less_than_operator	reduce 167',
    '	equals_operator	reduce 167',
    '	greater_than_operator	reduce 167',
    '	underscore	reduce 167',
    '	_ALTER	reduce 167',
    '	_AND	reduce 167',
    '	_AS	reduce 167',
    '	_AT	reduce 167',
    '	_BETWEEN	reduce 167',
    '	_CHECK	reduce 167',
    '	_COLLATE	reduce 167',
    '	_COMMIT	reduce 167',
    '	_CONNECT	reduce 167',
    '	_CONSTRAINT	reduce 167',
    '	_CREATE	reduce 167',
    '	_CROSS	reduce 167',
    '	_DAY	reduce 167',
    '	_DECLARE	reduce 167',
    '	_DEFAULT	reduce 167',
    '	_DELETE	reduce 167',
    '	_DISCONNECT	reduce 167',
    '	_DROP	reduce 167',
    '	_ELSE	reduce 167',
    '	_END	reduce 167',
    '	_ESCAPE	reduce 167',
    '	_EXCEPT	reduce 167',
    '	_FOR	reduce 167',
    '	_FROM	reduce 167',
    '	_FULL	reduce 167',
    '	_GRANT	reduce 167',
    '	_GROUP	reduce 167',
    '	_HAVING	reduce 167',
    '	_HOUR	reduce 167',
    '	_IN	reduce 167',
    '	_INNER	reduce 167',
    '	_INSERT	reduce 167',
    '	_INTERSECT	reduce 167',
    '	_INTO	reduce 167',
    '	_IS	reduce 167',
    '	_JOIN	reduce 167',
    '	_LEFT	reduce 167',
    '	_LIKE	reduce 167',
    '	_MATCH	reduce 167',
    '	_MINUTE	reduce 167',
    '	_MONTH	reduce 167',
    '	_NATURAL	reduce 167',
    '	_NOT	reduce 167',
    '	_OR	reduce 167',
    '	_ORDER	reduce 167',
    '	_OVERLAPS	reduce 167',
    '	_PRIMARY	reduce 167',
    '	_REFERENCES	reduce 167',
    '	_REVOKE	reduce 167',
    '	_RIGHT	reduce 167',
    '	_ROLLBACK	reduce 167',
    '	_SECOND	reduce 167',
    '	_SELECT	reduce 167',
    '	_SET	reduce 167',
    '	_TABLE	reduce 167',
    '	_THEN	reduce 167',
    '	_UNION	reduce 167',
    '	_UNIQUE	reduce 167',
    '	_UPDATE	reduce 167',
    '	_USER	reduce 167',
    '	_USING	reduce 167',
    '	_VALUES	reduce 167',
    '	_WHEN	reduce 167',
    '	_WHERE	reduce 167',
    '	_WITH	reduce 167',
    '	_YEAR	reduce 167',
    '	.	error',
    '',
    'state 411:',
    '',
    '	postfix_op : interval_qualifier _	(305)',
    '',
    '	.	reduce 305',
    '',
    'state 412:',
    '',
    '	postfix_op : collate_clause _	(306)',
    '',
    '	.	reduce 306',
    '',
    'state 413:',
    '',
    '	time_zone : _AT _ time_zone_specifier',
    '',
    '	_LOCAL	shift 598',
    '	_TIME	shift 599',
    '	.	error',
    '',
    '	time_zone_specifier	goto 597',
    '',
    'state 414:',
    '',
    '	collate_clause : _COLLATE _ collation_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	collation_name	goto 600',
    '	qualified_name	goto 313',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 415:',
    '',
    '	non_second_datetime_field : _DAY _	(174)',
    '',
    '	.	reduce 174',
    '',
    'state 416:',
    '',
    '	non_second_datetime_field : _HOUR _	(175)',
    '',
    '	.	reduce 175',
    '',
    'state 417:',
    '',
    '	non_second_datetime_field : _MINUTE _	(176)',
    '',
    '	.	reduce 176',
    '',
    'state 418:',
    '',
    '	non_second_datetime_field : _MONTH _	(173)',
    '',
    '	.	reduce 173',
    '',
    'state 419:',
    '',
    '	*** conflicts:',
    '',
    '	shift 602, reduce 182 on left_paren',
    '',
    '	interval_qualifier : _SECOND _ single_datetime_field_opt',
    '	single_datetime_field_opt : _	(182)',
    '',
    '	left_paren	shift 602',
    '	$end	reduce 182',
    '	identifier_body	reduce 182',
    '	delimited_identifier	reduce 182',
    '	not_equals_operator	reduce 182',
    '	greater_than_or_equals_operator	reduce 182',
    '	less_than_or_equals_operator	reduce 182',
    '	concatenation_operator	reduce 182',
    '	right_paren	reduce 182',
    '	asterisk	reduce 182',
    '	plus_sign	reduce 182',
    '	comma	reduce 182',
    '	minus_sign	reduce 182',
    '	solidus	reduce 182',
    '	semicolon	reduce 182',
    '	less_than_operator	reduce 182',
    '	equals_operator	reduce 182',
    '	greater_than_operator	reduce 182',
    '	underscore	reduce 182',
    '	_ALTER	reduce 182',
    '	_AND	reduce 182',
    '	_AS	reduce 182',
    '	_AT	reduce 182',
    '	_BETWEEN	reduce 182',
    '	_CHECK	reduce 182',
    '	_COLLATE	reduce 182',
    '	_COMMIT	reduce 182',
    '	_CONNECT	reduce 182',
    '	_CONSTRAINT	reduce 182',
    '	_CREATE	reduce 182',
    '	_CROSS	reduce 182',
    '	_DAY	reduce 182',
    '	_DECLARE	reduce 182',
    '	_DEFAULT	reduce 182',
    '	_DELETE	reduce 182',
    '	_DISCONNECT	reduce 182',
    '	_DROP	reduce 182',
    '	_ELSE	reduce 182',
    '	_END	reduce 182',
    '	_ESCAPE	reduce 182',
    '	_EXCEPT	reduce 182',
    '	_FOR	reduce 182',
    '	_FROM	reduce 182',
    '	_FULL	reduce 182',
    '	_GRANT	reduce 182',
    '	_GROUP	reduce 182',
    '	_HAVING	reduce 182',
    '	_HOUR	reduce 182',
    '	_IN	reduce 182',
    '	_INNER	reduce 182',
    '	_INSERT	reduce 182',
    '	_INTERSECT	reduce 182',
    '	_INTO	reduce 182',
    '	_IS	reduce 182',
    '	_JOIN	reduce 182',
    '	_LEFT	reduce 182',
    '	_LIKE	reduce 182',
    '	_MATCH	reduce 182',
    '	_MINUTE	reduce 182',
    '	_MONTH	reduce 182',
    '	_NATURAL	reduce 182',
    '	_NOT	reduce 182',
    '	_OR	reduce 182',
    '	_ORDER	reduce 182',
    '	_OVERLAPS	reduce 182',
    '	_PRIMARY	reduce 182',
    '	_REFERENCES	reduce 182',
    '	_REVOKE	reduce 182',
    '	_RIGHT	reduce 182',
    '	_ROLLBACK	reduce 182',
    '	_SECOND	reduce 182',
    '	_SELECT	reduce 182',
    '	_SET	reduce 182',
    '	_TABLE	reduce 182',
    '	_THEN	reduce 182',
    '	_UNION	reduce 182',
    '	_UNIQUE	reduce 182',
    '	_UPDATE	reduce 182',
    '	_USER	reduce 182',
    '	_USING	reduce 182',
    '	_VALUES	reduce 182',
    '	_WHEN	reduce 182',
    '	_WHERE	reduce 182',
    '	_WITH	reduce 182',
    '	_YEAR	reduce 182',
    '	.	error',
    '',
    '	single_datetime_field_opt	goto 601',
    '',
    'state 420:',
    '',
    '	non_second_datetime_field : _YEAR _	(172)',
    '',
    '	.	reduce 172',
    '',
    'state 421:',
    '',
    '	expression : expression concatenation_operator _ multiplicative_expression',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 603',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 422:',
    '',
    '	expression : expression plus_sign _ multiplicative_expression',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 604',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 423:',
    '',
    '	expression : expression minus_sign _ multiplicative_expression',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 605',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 424:',
    '',
    '	character_string_literal : introducer character_set_specification _ character_string_literal_main',
    '	identifier : introducer character_set_specification _ actual_identifier',
    '',
    '	identifier_body	shift 66',
    '	string_literal_continuation	shift 145',
    '	delimited_identifier	shift 67',
    '	.	error',
    '',
    '	actual_identifier	goto 299',
    '	character_string_literal_main	goto 516',
    '	regular_identifier	goto 64',
    '',
    'state 425:',
    '',
    '	subquery : query_expression _	(319)',
    '	query_expression : query_expression _ _UNION all_opt corresponding_spec_opt query_term',
    '	query_expression : query_expression _ _EXCEPT all_opt corresponding_spec_opt query_term',
    '',
    '	_EXCEPT	shift 91',
    '	_UNION	shift 93',
    '	right_paren	reduce 319',
    '	.	error',
    '',
    'state 426:',
    '',
    '	scalar_subquery : left_paren subquery _ right_paren',
    '',
    '	right_paren	shift 606',
    '	.	error',
    '',
    'state 427:',
    '',
    '	row_value_constructor : left_paren row_value_constructor_list _ right_paren',
    '	row_value_constructor_list : row_value_constructor_list _ comma expression',
    '',
    '	right_paren	shift 607',
    '	comma	shift 608',
    '	.	error',
    '',
    'state 428:',
    '',
    '	*** conflicts:',
    '',
    '	shift 609, reduce 514 on right_paren',
    '',
    '	primary_expression : left_paren expression _ right_paren',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	row_value_constructor_list : expression _	(514)',
    '',
    '	concatenation_operator	shift 421',
    '	right_paren	shift 609',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	comma	reduce 514',
    '	.	error',
    '',
    'state 429:',
    '',
    '	primary_expression : left_paren _ expression right_paren',
    '	scalar_subquery : left_paren _ subquery right_paren',
    '	table_subquery : left_paren _ query_expression right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 429',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SELECT	shift 83',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TABLE	shift 85',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_VALUES	shift 87',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	explicit_table	goto 49',
    '	table_value_constructor	goto 50',
    '	query_specification	goto 51',
    '	table_subquery	goto 52',
    '	simple_table	goto 53',
    '	non_join_query_primary	goto 54',
    '	query_term	goto 55',
    '	non_join_query_term	goto 56',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	query_expression	goto 610',
    '	subquery	goto 426',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 567',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 430:',
    '',
    '	unary_expression : plus_sign postfix_expression _	(307)',
    '',
    '	.	reduce 307',
    '',
    'state 431:',
    '',
    '	unary_expression : minus_sign postfix_expression _	(308)',
    '',
    '	.	reduce 308',
    '',
    'state 432:',
    '',
    '	bit_length_expression : _BIT_LENGTH left_paren _ expression right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 611',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 433:',
    '',
    '	searched_case : _CASE searched_when_clause _ else_clause_opt _END',
    '	else_clause_opt : _	(450)',
    '',
    '	_ELSE	shift 614',
    '	_END	reduce 450',
    '	.	error',
    '',
    '	else_clause	goto 612',
    '	else_clause_opt	goto 613',
    '',
    'state 434:',
    '',
    '	simple_case : _CASE case_operand _ simple_when_clause else_clause_opt _END',
    '',
    '	_WHEN	shift 616',
    '	.	error',
    '',
    '	simple_when_clause	goto 615',
    '',
    'state 435:',
    '',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	case_operand : expression _	(452)',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	_WHEN	reduce 452',
    '	.	error',
    '',
    'state 436:',
    '',
    '	searched_when_clause : _WHEN _ search_condition _THEN result',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 636',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXISTS	shift 637',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NOT	shift 638',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UNIQUE	shift 639',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	row_value_constructor_1	goto 617',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 618',
    '	row_value_constructor	goto 619',
    '	overlaps_predicate	goto 620',
    '	match_predicate	goto 621',
    '	unique_predicate	goto 622',
    '	exists_predicate	goto 623',
    '	quantified_comparison_predicate	goto 624',
    '	null_predicate	goto 625',
    '	like_predicate	goto 626',
    '	in_predicate	goto 627',
    '	between_predicate	goto 628',
    '	comparison_predicate	goto 629',
    '	predicate	goto 630',
    '	boolean_primary	goto 631',
    '	boolean_test	goto 632',
    '	boolean_factor	goto 633',
    '	boolean_term	goto 634',
    '	search_condition	goto 635',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 437:',
    '',
    '	cast_specification : _CAST left_paren _ cast_operand _AS cast_target right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	cast_operand	goto 640',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 641',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 438:',
    '',
    '	case_abbreviation : _COALESCE left_paren _ expression_list right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	expression_list	goto 642',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 643',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 439:',
    '',
    '	form_of_use_conversion : _CONVERT left_paren _ expression _USING form_of_use_conversion_name right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 644',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 440:',
    '',
    '	current_time_value_function : _CURRENT_TIME left_paren _ time_precision right_paren',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	time_fractional_seconds_precision	goto 645',
    '	time_precision	goto 646',
    '	unsigned_integer	goto 647',
    '',
    'state 441:',
    '',
    '	current_timestamp_value_function : _CURRENT_TIMESTAMP left_paren _ timestamp_precision right_paren',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	time_fractional_seconds_precision	goto 648',
    '	timestamp_precision	goto 649',
    '	unsigned_integer	goto 647',
    '',
    'state 442:',
    '',
    '	extract_expression : _EXTRACT left_paren _ extract_field _FROM extract_source right_paren',
    '',
    '	_DAY	shift 415',
    '	_HOUR	shift 416',
    '	_MINUTE	shift 417',
    '	_MONTH	shift 418',
    '	_SECOND	shift 654',
    '	_TIMEZONE_HOUR	shift 655',
    '	_TIMEZONE_MINUTE	shift 656',
    '	_YEAR	shift 420',
    '	.	error',
    '',
    '	time_zone_field	goto 650',
    '	datetime_field	goto 651',
    '	extract_field	goto 652',
    '	non_second_datetime_field	goto 653',
    '',
    'state 443:',
    '',
    '	fold : _LOWER left_paren _ expression right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 657',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 444:',
    '',
    '	case_abbreviation : _NULLIF left_paren _ expression comma expression right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 658',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 445:',
    '',
    '	octet_length_expression : _OCTET_LENGTH left_paren _ expression right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 659',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 446:',
    '',
    '	position_expression : _POSITION left_paren _ expression _IN expression right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 660',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 447:',
    '',
    '	character_bit_substring_function : _SUBSTRING left_paren _ expression _FROM start_position for_strlength_opt right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 661',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 448:',
    '',
    '	character_translation : _TRANSLATE left_paren _ expression _USING translation_name right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 662',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 449:',
    '',
    '	trim_function : _TRIM left_paren _ trim_operands right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_BOTH	shift 668',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LEADING	shift 669',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRAILING	shift 670',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_character	goto 663',
    '	trim_specification	goto 664',
    '	trim_source	goto 665',
    '	trim_operands	goto 666',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 667',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 450:',
    '',
    '	fold : _UPPER left_paren _ expression right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 671',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 451:',
    '',
    '	corresponding_spec_opt : corresponding_spec _	(359)',
    '',
    '	.	reduce 359',
    '',
    'state 452:',
    '',
    '	non_join_query_term : query_term _INTERSECT all_opt corresponding_spec_opt _ query_primary',
    '',
    '	left_paren	shift 68',
    '	_SELECT	shift 83',
    '	_TABLE	shift 85',
    '	_VALUES	shift 87',
    '	.	error',
    '',
    '	explicit_table	goto 49',
    '	table_value_constructor	goto 50',
    '	query_specification	goto 51',
    '	table_subquery	goto 52',
    '	simple_table	goto 53',
    '	query_primary	goto 672',
    '	non_join_query_primary	goto 673',
    '',
    'state 453:',
    '',
    '	corresponding_spec : _CORRESPONDING _ corresponding_column_list_opt',
    '	corresponding_column_list_opt : _	(437)',
    '',
    '	_BY	shift 675',
    '	left_paren	reduce 437',
    '	_SELECT	reduce 437',
    '	_TABLE	reduce 437',
    '	_VALUES	reduce 437',
    '	.	error',
    '',
    '	corresponding_column_list_opt	goto 674',
    '',
    'state 454:',
    '',
    '	query_expression : query_expression _EXCEPT all_opt corresponding_spec_opt _ query_term',
    '',
    '	left_paren	shift 68',
    '	_SELECT	shift 83',
    '	_TABLE	shift 85',
    '	_VALUES	shift 87',
    '	.	error',
    '',
    '	explicit_table	goto 49',
    '	table_value_constructor	goto 50',
    '	query_specification	goto 51',
    '	table_subquery	goto 52',
    '	simple_table	goto 53',
    '	non_join_query_primary	goto 54',
    '	query_term	goto 676',
    '	non_join_query_term	goto 677',
    '',
    'state 455:',
    '',
    '	sort_specification : sort_key _ collate_clause_opt ordering_specification_opt',
    '	collate_clause_opt : _	(98)',
    '',
    '	_COLLATE	shift 414',
    '	$end	reduce 98',
    '	identifier_body	reduce 98',
    '	delimited_identifier	reduce 98',
    '	left_paren	reduce 98',
    '	comma	reduce 98',
    '	underscore	reduce 98',
    '	_ALTER	reduce 98',
    '	_ASC	reduce 98',
    '	_COMMIT	reduce 98',
    '	_CONNECT	reduce 98',
    '	_CREATE	reduce 98',
    '	_DECLARE	reduce 98',
    '	_DELETE	reduce 98',
    '	_DESC	reduce 98',
    '	_DISCONNECT	reduce 98',
    '	_DROP	reduce 98',
    '	_FOR	reduce 98',
    '	_GRANT	reduce 98',
    '	_INSERT	reduce 98',
    '	_REVOKE	reduce 98',
    '	_ROLLBACK	reduce 98',
    '	_SELECT	reduce 98',
    '	_SET	reduce 98',
    '	_TABLE	reduce 98',
    '	_UPDATE	reduce 98',
    '	_VALUES	reduce 98',
    '	.	error',
    '',
    '	collate_clause	goto 678',
    '	collate_clause_opt	goto 679',
    '',
    'state 456:',
    '',
    '	sort_specification_list : sort_specification _	(589)',
    '',
    '	.	reduce 589',
    '',
    'state 457:',
    '',
    '	order_by_clause_opt : _ORDER _BY sort_specification_list _	(588)',
    '	sort_specification_list : sort_specification_list _ comma sort_specification',
    '',
    '	comma	shift 680',
    '	$end	reduce 588',
    '	identifier_body	reduce 588',
    '	delimited_identifier	reduce 588',
    '	left_paren	reduce 588',
    '	underscore	reduce 588',
    '	_ALTER	reduce 588',
    '	_COMMIT	reduce 588',
    '	_CONNECT	reduce 588',
    '	_CREATE	reduce 588',
    '	_DECLARE	reduce 588',
    '	_DELETE	reduce 588',
    '	_DISCONNECT	reduce 588',
    '	_DROP	reduce 588',
    '	_FOR	reduce 588',
    '	_GRANT	reduce 588',
    '	_INSERT	reduce 588',
    '	_REVOKE	reduce 588',
    '	_ROLLBACK	reduce 588',
    '	_SELECT	reduce 588',
    '	_SET	reduce 588',
    '	_TABLE	reduce 588',
    '	_UPDATE	reduce 588',
    '	_VALUES	reduce 588',
    '	.	error',
    '',
    'state 458:',
    '',
    '	sort_key : column_name _	(592)',
    '',
    '	.	reduce 592',
    '',
    'state 459:',
    '',
    '	column_name : identifier _	(100)',
    '',
    '	.	reduce 100',
    '',
    'state 460:',
    '',
    '	unsigned_integer : unsigned_integer _ digit',
    '	sort_key : unsigned_integer _	(593)',
    '',
    '	digit	shift 331',
    '	$end	reduce 593',
    '	identifier_body	reduce 593',
    '	delimited_identifier	reduce 593',
    '	left_paren	reduce 593',
    '	comma	reduce 593',
    '	underscore	reduce 593',
    '	_ALTER	reduce 593',
    '	_ASC	reduce 593',
    '	_COLLATE	reduce 593',
    '	_COMMIT	reduce 593',
    '	_CONNECT	reduce 593',
    '	_CREATE	reduce 593',
    '	_DECLARE	reduce 593',
    '	_DELETE	reduce 593',
    '	_DESC	reduce 593',
    '	_DISCONNECT	reduce 593',
    '	_DROP	reduce 593',
    '	_FOR	reduce 593',
    '	_GRANT	reduce 593',
    '	_INSERT	reduce 593',
    '	_REVOKE	reduce 593',
    '	_ROLLBACK	reduce 593',
    '	_SELECT	reduce 593',
    '	_SET	reduce 593',
    '	_TABLE	reduce 593',
    '	_UPDATE	reduce 593',
    '	_VALUES	reduce 593',
    '	.	error',
    '',
    'state 461:',
    '',
    '	query_expression : query_expression _UNION all_opt corresponding_spec_opt _ query_term',
    '',
    '	left_paren	shift 68',
    '	_SELECT	shift 83',
    '	_TABLE	shift 85',
    '	_VALUES	shift 87',
    '	.	error',
    '',
    '	explicit_table	goto 49',
    '	table_value_constructor	goto 50',
    '	query_specification	goto 51',
    '	table_subquery	goto 52',
    '	simple_table	goto 53',
    '	non_join_query_primary	goto 54',
    '	query_term	goto 681',
    '	non_join_query_term	goto 677',
    '',
    'state 462:',
    '',
    '	module_contents : procedure _	(579)',
    '',
    '	.	reduce 579',
    '',
    'state 463:',
    '',
    '	module_contents : dynamic_declare_cursor _	(578)',
    '',
    '	.	reduce 578',
    '',
    'state 464:',
    '',
    '	module_contents : declare_cursor _	(577)',
    '',
    '	.	reduce 577',
    '',
    'state 465:',
    '',
    '	module_opt : module_contents _	(63)',
    '',
    '	.	reduce 63',
    '',
    'state 466:',
    '',
    '	module_opt : temporary_table_declaration _ module_contents',
    '',
    '	_DECLARE	shift 683',
    '	_PROCEDURE	shift 469',
    '	.	error',
    '',
    '	procedure	goto 462',
    '	dynamic_declare_cursor	goto 463',
    '	declare_cursor	goto 464',
    '	module_contents	goto 682',
    '',
    'state 467:',
    '',
    '	module : module_name_clause language_clause module_authorization_clause module_opt _	(61)',
    '',
    '	.	reduce 61',
    '',
    'state 468:',
    '',
    '	temporary_table_declaration : _DECLARE _ _LOCAL _TEMPORARY _TABLE qualified_local_table_name table_element_list temporary_table_declaration_opt',
    '	declare_cursor : _DECLARE _ cursor_name insensitive_opt scroll_opt _CURSOR _FOR cursor_specification',
    '	dynamic_declare_cursor : _DECLARE _ cursor_name insensitive_opt scroll_opt _CURSOR _FOR statement_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_LOCAL	shift 116',
    '	.	error',
    '',
    '	cursor_name	goto 684',
    '	actual_identifier	goto 61',
    '	identifier	goto 685',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 469:',
    '',
    '	procedure : _PROCEDURE _ procedure_name parameter_declaration_list semicolon SQL_procedure_statement semicolon',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	procedure_name	goto 686',
    '	actual_identifier	goto 61',
    '	identifier	goto 687',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 470:',
    '',
    '	module_authorization_identifier : authorization_identifier _	(78)',
    '',
    '	.	reduce 78',
    '',
    'state 471:',
    '',
    '	module_authorization_clause : _AUTHORIZATION module_authorization_identifier _	(76)',
    '',
    '	.	reduce 76',
    '',
    'state 472:',
    '',
    '	authorization_identifier : identifier _	(79)',
    '',
    '	.	reduce 79',
    '',
    'state 473:',
    '',
    '	module_authorization_clause : _SCHEMA schema_name _	(75)',
    '	module_authorization_clause : _SCHEMA schema_name _ _AUTHORIZATION module_authorization_identifier',
    '',
    '	_AUTHORIZATION	shift 688',
    '	_DECLARE	reduce 75',
    '	_PROCEDURE	reduce 75',
    '	.	error',
    '',
    'state 474:',
    '',
    '	character_set_name : identifier period SQL_language_identifier _	(34)',
    '',
    '	.	reduce 34',
    '',
    'state 475:',
    '',
    '	character_set_name : identifier period identifier _ period SQL_language_identifier',
    '',
    '	period	shift 689',
    '	.	error',
    '',
    'state 476:',
    '',
    '	alter_domain_action : drop_domain_constraint_definition _	(768)',
    '',
    '	.	reduce 768',
    '',
    'state 477:',
    '',
    '	alter_domain_action : add_domain_constraint_definition _	(767)',
    '',
    '	.	reduce 767',
    '',
    'state 478:',
    '',
    '	alter_domain_action : drop_domain_default_clause _	(766)',
    '',
    '	.	reduce 766',
    '',
    'state 479:',
    '',
    '	alter_domain_action : set_domain_default_clause _	(765)',
    '',
    '	.	reduce 765',
    '',
    'state 480:',
    '',
    '	alter_domain_statement : _ALTER _DOMAIN domain_name alter_domain_action _	(764)',
    '',
    '	.	reduce 764',
    '',
    'state 481:',
    '',
    '	add_domain_constraint_definition : _ADD _ domain_constraint',
    '	constraint_name_definition_opt : _	(229)',
    '',
    '	_CONSTRAINT	shift 693',
    '	_CHECK	reduce 229',
    '	.	error',
    '',
    '	domain_constraint	goto 690',
    '	constraint_name_definition	goto 691',
    '	constraint_name_definition_opt	goto 692',
    '',
    'state 482:',
    '',
    '	drop_domain_default_clause : _DROP _ _DEFAULT',
    '	drop_domain_constraint_definition : _DROP _ _CONSTRAINT constraint_name',
    '',
    '	_CONSTRAINT	shift 694',
    '	_DEFAULT	shift 695',
    '	.	error',
    '',
    'state 483:',
    '',
    '	set_domain_default_clause : _SET _ default_clause',
    '',
    '	_DEFAULT	shift 697',
    '	.	error',
    '',
    '	default_clause	goto 696',
    '',
    'state 484:',
    '',
    '	alter_table_action : drop_table_constraint_definition _	(747)',
    '',
    '	.	reduce 747',
    '',
    'state 485:',
    '',
    '	alter_table_action : add_table_constraint_definition _	(746)',
    '',
    '	.	reduce 746',
    '',
    'state 486:',
    '',
    '	alter_table_action : drop_column_definition _	(745)',
    '',
    '	.	reduce 745',
    '',
    'state 487:',
    '',
    '	alter_table_action : alter_column_definition _	(744)',
    '',
    '	.	reduce 744',
    '',
    'state 488:',
    '',
    '	alter_table_action : add_column_definition _	(743)',
    '',
    '	.	reduce 743',
    '',
    'state 489:',
    '',
    '	alter_table_statement : _ALTER _TABLE table_name alter_table_action _	(742)',
    '',
    '	.	reduce 742',
    '',
    'state 490:',
    '',
    '	add_column_definition : _ADD _ column_opt column_definition',
    '	add_table_constraint_definition : _ADD _ table_constraint_definition',
    '	constraint_name_definition_opt : _	(229)',
    '	column_opt : _	(748)',
    '',
    '	_COLUMN	shift 701',
    '	_CONSTRAINT	shift 693',
    '	_CHECK	reduce 229',
    '	_FOREIGN	reduce 229',
    '	_PRIMARY	reduce 229',
    '	_UNIQUE	reduce 229',
    '	identifier_body	reduce 748',
    '	delimited_identifier	reduce 748',
    '	underscore	reduce 748',
    '	.	error',
    '',
    '	column_opt	goto 698',
    '	constraint_name_definition	goto 691',
    '	constraint_name_definition_opt	goto 699',
    '	table_constraint_definition	goto 700',
    '',
    'state 491:',
    '',
    '	alter_column_definition : _ALTER _ column_opt column_name alter_column_action',
    '	column_opt : _	(748)',
    '',
    '	_COLUMN	shift 701',
    '	identifier_body	reduce 748',
    '	delimited_identifier	reduce 748',
    '	underscore	reduce 748',
    '	.	error',
    '',
    '	column_opt	goto 702',
    '',
    'state 492:',
    '',
    '	drop_column_definition : _DROP _ column_opt column_name drop_behaviour',
    '	drop_table_constraint_definition : _DROP _ _CONSTRAINT constraint_name drop_behaviour',
    '	column_opt : _	(748)',
    '',
    '	_COLUMN	shift 701',
    '	_CONSTRAINT	shift 704',
    '	identifier_body	reduce 748',
    '	delimited_identifier	reduce 748',
    '	underscore	reduce 748',
    '	.	error',
    '',
    '	column_opt	goto 703',
    '',
    'state 493:',
    '',
    '	connection_target : SQL_server_name connection_name_opt _ user_name_opt',
    '	user_name_opt : _	(860)',
    '',
    '	_USER	shift 706',
    '	$end	reduce 860',
    '	identifier_body	reduce 860',
    '	delimited_identifier	reduce 860',
    '	left_paren	reduce 860',
    '	semicolon	reduce 860',
    '	underscore	reduce 860',
    '	_ALTER	reduce 860',
    '	_COMMIT	reduce 860',
    '	_CONNECT	reduce 860',
    '	_CREATE	reduce 860',
    '	_DECLARE	reduce 860',
    '	_DELETE	reduce 860',
    '	_DISCONNECT	reduce 860',
    '	_DROP	reduce 860',
    '	_GRANT	reduce 860',
    '	_INSERT	reduce 860',
    '	_REVOKE	reduce 860',
    '	_ROLLBACK	reduce 860',
    '	_SELECT	reduce 860',
    '	_SET	reduce 860',
    '	_TABLE	reduce 860',
    '	_UPDATE	reduce 860',
    '	_VALUES	reduce 860',
    '	.	error',
    '',
    '	user_name_opt	goto 705',
    '',
    'state 494:',
    '',
    '	connection_name_opt : _AS _ connection_name',
    '',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	digit	shift 147',
    '	plus_sign	shift 148',
    '	minus_sign	shift 149',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_DATE	shift 154',
    '	_INTERVAL	shift 156',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	.	error',
    '',
    '	connection_name	goto 707',
    '	simple_value_specification	goto 121',
    '	parameter_name	goto 122',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 128',
    '	signed_numeric_literal	goto 129',
    '	literal	goto 130',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 132',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	sign	goto 137',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 142',
    '',
    'state 495:',
    '',
    '	table_definition : _CREATE table_definition_opts _TABLE table_name _ table_element_list table_commit_opts',
    '',
    '	left_paren	shift 709',
    '	.	error',
    '',
    '	table_element_list	goto 708',
    '',
    'state 496:',
    '',
    '	assertion_definition : _CREATE _ASSERTION constraint_name assertion_check _ constraint_attributes_opt',
    '	constraint_attributes_opt : _	(558)',
    '',
    '	_DEFERRABLE	shift 713',
    '	_INITIALLY	shift 714',
    '	$end	reduce 558',
    '	identifier_body	reduce 558',
    '	delimited_identifier	reduce 558',
    '	left_paren	reduce 558',
    '	semicolon	reduce 558',
    '	underscore	reduce 558',
    '	_ALTER	reduce 558',
    '	_COMMIT	reduce 558',
    '	_CONNECT	reduce 558',
    '	_CREATE	reduce 558',
    '	_DECLARE	reduce 558',
    '	_DELETE	reduce 558',
    '	_DISCONNECT	reduce 558',
    '	_DROP	reduce 558',
    '	_GRANT	reduce 558',
    '	_INSERT	reduce 558',
    '	_REVOKE	reduce 558',
    '	_ROLLBACK	reduce 558',
    '	_SELECT	reduce 558',
    '	_SET	reduce 558',
    '	_TABLE	reduce 558',
    '	_UPDATE	reduce 558',
    '	_VALUES	reduce 558',
    '	.	error',
    '',
    '	constraint_check_time	goto 710',
    '	constraint_attributes	goto 711',
    '	constraint_attributes_opt	goto 712',
    '',
    'state 497:',
    '',
    '	assertion_check : _CHECK _ left_paren search_condition right_paren',
    '',
    '	left_paren	shift 715',
    '	.	error',
    '',
    'state 498:',
    '',
    '	character_set_definition : _CREATE _CHARACTER _SET character_set_name _ as_opt character_set_source charset_collation_opt',
    '	as_opt : _	(394)',
    '',
    '	_AS	shift 501',
    '	_GET	reduce 394',
    '	.	error',
    '',
    '	as_opt	goto 716',
    '',
    'state 499:',
    '',
    '	collation_definition : _CREATE _COLLATION collation_name _FOR _ character_set_specification _FROM collation_source pad_attribute_opt',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	actual_identifier	goto 61',
    '	SQL_language_identifier	goto 96',
    '	identifier	goto 97',
    '	character_set_name	goto 98',
    '	character_set_specification	goto 717',
    '	introducer	goto 63',
    '	regular_identifier	goto 100',
    '',
    'state 500:',
    '',
    '	domain_definition : _CREATE _DOMAIN domain_name as_opt _ data_type default_clause_opt domain_constraint_opt collate_clause_opt',
    '',
    '	_BIT	shift 727',
    '	_CHAR	shift 728',
    '	_CHARACTER	shift 729',
    '	_DATE	shift 730',
    '	_DEC	shift 731',
    '	_DECIMAL	shift 732',
    '	_DOUBLE	shift 733',
    '	_FLOAT	shift 734',
    '	_INT	shift 735',
    '	_INTEGER	shift 736',
    '	_INTERVAL	shift 737',
    '	_NATIONAL	shift 738',
    '	_NCHAR	shift 739',
    '	_NUMERIC	shift 740',
    '	_REAL	shift 741',
    '	_SMALLINT	shift 742',
    '	_TIME	shift 743',
    '	_TIMESTAMP	shift 744',
    '	_VARCHAR	shift 745',
    '	.	error',
    '',
    '	approximate_numeric_type	goto 718',
    '	exact_numeric_type	goto 719',
    '	interval_type	goto 720',
    '	datetime_type	goto 721',
    '	numeric_type	goto 722',
    '	bit_string_type	goto 723',
    '	national_character_string_type	goto 724',
    '	character_string_type	goto 725',
    '	data_type	goto 726',
    '',
    'state 501:',
    '',
    '	as_opt : _AS _	(395)',
    '',
    '	.	reduce 395',
    '',
    'state 502:',
    '',
    '	schema_character_set_specification_opt : schema_character_set_specification _	(631)',
    '',
    '	.	reduce 631',
    '',
    'state 503:',
    '',
    '	schema_definition : _CREATE _SCHEMA schema_name_clause schema_character_set_specification_opt _ schema_elements',
    '',
    '	_CREATE	shift 756',
    '	_GRANT	shift 78',
    '	.	error',
    '',
    '	schema_element	goto 746',
    '	schema_elements	goto 747',
    '	assertion_definition	goto 748',
    '	translation_definition	goto 749',
    '	collation_definition	goto 750',
    '	character_set_definition	goto 751',
    '	domain_definition	goto 752',
    '	grant_statement	goto 753',
    '	view_definition	goto 754',
    '	table_definition	goto 755',
    '',
    'state 504:',
    '',
    '	schema_character_set_specification : _DEFAULT _ _CHARACTER _SET character_set_specification',
    '',
    '	_CHARACTER	shift 757',
    '	.	error',
    '',
    'state 505:',
    '',
    '	schema_name_clause : schema_name _AUTHORIZATION _ schema_authorization_identifier',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	schema_authorization_identifier	goto 758',
    '	authorization_identifier	goto 508',
    '	actual_identifier	goto 61',
    '	identifier	goto 472',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 506:',
    '',
    '	schema_name : identifier period _ identifier',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	actual_identifier	goto 61',
    '	identifier	goto 759',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 507:',
    '',
    '	schema_name_clause : _AUTHORIZATION schema_authorization_identifier _	(635)',
    '',
    '	.	reduce 635',
    '',
    'state 508:',
    '',
    '	schema_authorization_identifier : authorization_identifier _	(637)',
    '',
    '	.	reduce 637',
    '',
    'state 509:',
    '',
    '	translation_definition : _CREATE _TRANSLATION translation_name _FOR _ source_character_set_specification _TO target_character_set_specification _FROM translation_source',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	source_character_set_specification	goto 760',
    '	actual_identifier	goto 61',
    '	SQL_language_identifier	goto 96',
    '	identifier	goto 97',
    '	character_set_name	goto 98',
    '	character_set_specification	goto 761',
    '	introducer	goto 63',
    '	regular_identifier	goto 100',
    '',
    'state 510:',
    '',
    '	view_definition : _CREATE _VIEW table_name view_column_list_opt _ _AS query_expression view_check_opt',
    '',
    '	_AS	shift 762',
    '	.	error',
    '',
    'state 511:',
    '',
    '	view_column_list_opt : left_paren _ view_column_list right_paren',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	view_column_list	goto 763',
    '	column_name_list	goto 764',
    '	column_name	goto 551',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 512:',
    '',
    '	temporary_table_declaration : _DECLARE _LOCAL _TEMPORARY _TABLE _ qualified_local_table_name table_element_list temporary_table_declaration_opt',
    '',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	qualified_local_table_name	goto 765',
    '',
    'state 513:',
    '',
    '	where_clause_opt : where_clause _	(378)',
    '',
    '	.	reduce 378',
    '',
    'state 514:',
    '',
    '	delete_statement__searched : _DELETE _FROM table_name where_clause_opt _	(809)',
    '',
    '	.	reduce 809',
    '',
    'state 515:',
    '',
    '	where_clause : _WHERE _ search_condition',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 636',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXISTS	shift 637',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NOT	shift 638',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UNIQUE	shift 639',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	row_value_constructor_1	goto 617',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 618',
    '	row_value_constructor	goto 619',
    '	overlaps_predicate	goto 620',
    '	match_predicate	goto 621',
    '	unique_predicate	goto 622',
    '	exists_predicate	goto 623',
    '	quantified_comparison_predicate	goto 624',
    '	null_predicate	goto 625',
    '	like_predicate	goto 626',
    '	in_predicate	goto 627',
    '	between_predicate	goto 628',
    '	comparison_predicate	goto 629',
    '	predicate	goto 630',
    '	boolean_primary	goto 631',
    '	boolean_test	goto 632',
    '	boolean_factor	goto 633',
    '	boolean_term	goto 634',
    '	search_condition	goto 766',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 516:',
    '',
    '	character_string_literal : introducer character_set_specification character_string_literal_main _	(27)',
    '	character_string_literal_main : character_string_literal_main _ string_literal_continuation',
    '',
    '	string_literal_continuation	shift 326',
    '	$end	reduce 27',
    '	identifier_body	reduce 27',
    '	delimited_identifier	reduce 27',
    '	not_equals_operator	reduce 27',
    '	greater_than_or_equals_operator	reduce 27',
    '	less_than_or_equals_operator	reduce 27',
    '	concatenation_operator	reduce 27',
    '	left_paren	reduce 27',
    '	right_paren	reduce 27',
    '	asterisk	reduce 27',
    '	plus_sign	reduce 27',
    '	comma	reduce 27',
    '	minus_sign	reduce 27',
    '	solidus	reduce 27',
    '	semicolon	reduce 27',
    '	less_than_operator	reduce 27',
    '	equals_operator	reduce 27',
    '	greater_than_operator	reduce 27',
    '	underscore	reduce 27',
    '	_ALTER	reduce 27',
    '	_AND	reduce 27',
    '	_AS	reduce 27',
    '	_AT	reduce 27',
    '	_BETWEEN	reduce 27',
    '	_CHECK	reduce 27',
    '	_COLLATE	reduce 27',
    '	_COMMIT	reduce 27',
    '	_CONNECT	reduce 27',
    '	_CONSTRAINT	reduce 27',
    '	_CREATE	reduce 27',
    '	_CROSS	reduce 27',
    '	_DAY	reduce 27',
    '	_DECLARE	reduce 27',
    '	_DELETE	reduce 27',
    '	_DISCONNECT	reduce 27',
    '	_DROP	reduce 27',
    '	_ELSE	reduce 27',
    '	_END	reduce 27',
    '	_ESCAPE	reduce 27',
    '	_EXCEPT	reduce 27',
    '	_FOR	reduce 27',
    '	_FROM	reduce 27',
    '	_FULL	reduce 27',
    '	_GRANT	reduce 27',
    '	_GROUP	reduce 27',
    '	_HAVING	reduce 27',
    '	_HOUR	reduce 27',
    '	_IN	reduce 27',
    '	_INNER	reduce 27',
    '	_INSERT	reduce 27',
    '	_INTERSECT	reduce 27',
    '	_INTO	reduce 27',
    '	_IS	reduce 27',
    '	_JOIN	reduce 27',
    '	_LEFT	reduce 27',
    '	_LIKE	reduce 27',
    '	_MATCH	reduce 27',
    '	_MINUTE	reduce 27',
    '	_MONTH	reduce 27',
    '	_NATURAL	reduce 27',
    '	_NOT	reduce 27',
    '	_OR	reduce 27',
    '	_ORDER	reduce 27',
    '	_OVERLAPS	reduce 27',
    '	_PRIMARY	reduce 27',
    '	_REFERENCES	reduce 27',
    '	_REVOKE	reduce 27',
    '	_RIGHT	reduce 27',
    '	_ROLLBACK	reduce 27',
    '	_SECOND	reduce 27',
    '	_SELECT	reduce 27',
    '	_SET	reduce 27',
    '	_TABLE	reduce 27',
    '	_THEN	reduce 27',
    '	_UNION	reduce 27',
    '	_UNIQUE	reduce 27',
    '	_UPDATE	reduce 27',
    '	_USER	reduce 27',
    '	_USING	reduce 27',
    '	_VALUES	reduce 27',
    '	_WHEN	reduce 27',
    '	_WHERE	reduce 27',
    '	_WITH	reduce 27',
    '	_YEAR	reduce 27',
    '	.	error',
    '',
    'state 517:',
    '',
    '	signed_integer : sign _ unsigned_integer',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	unsigned_integer	goto 767',
    '',
    'state 518:',
    '',
    '	exponent : signed_integer _	(13)',
    '',
    '	.	reduce 13',
    '',
    'state 519:',
    '',
    '	approximate_numeric_literal : mantissa _E exponent _	(11)',
    '',
    '	.	reduce 11',
    '',
    'state 520:',
    '',
    '	unsigned_integer : unsigned_integer _ digit',
    '	signed_integer : unsigned_integer _	(15)',
    '',
    '	digit	shift 331',
    '	$end	reduce 15',
    '	identifier_body	reduce 15',
    '	delimited_identifier	reduce 15',
    '	not_equals_operator	reduce 15',
    '	greater_than_or_equals_operator	reduce 15',
    '	less_than_or_equals_operator	reduce 15',
    '	concatenation_operator	reduce 15',
    '	left_paren	reduce 15',
    '	right_paren	reduce 15',
    '	asterisk	reduce 15',
    '	plus_sign	reduce 15',
    '	comma	reduce 15',
    '	minus_sign	reduce 15',
    '	solidus	reduce 15',
    '	semicolon	reduce 15',
    '	less_than_operator	reduce 15',
    '	equals_operator	reduce 15',
    '	greater_than_operator	reduce 15',
    '	underscore	reduce 15',
    '	_ALTER	reduce 15',
    '	_AND	reduce 15',
    '	_AS	reduce 15',
    '	_AT	reduce 15',
    '	_BETWEEN	reduce 15',
    '	_CHECK	reduce 15',
    '	_COLLATE	reduce 15',
    '	_COMMIT	reduce 15',
    '	_CONNECT	reduce 15',
    '	_CONSTRAINT	reduce 15',
    '	_CREATE	reduce 15',
    '	_CROSS	reduce 15',
    '	_DAY	reduce 15',
    '	_DECLARE	reduce 15',
    '	_DELETE	reduce 15',
    '	_DISCONNECT	reduce 15',
    '	_DROP	reduce 15',
    '	_ELSE	reduce 15',
    '	_END	reduce 15',
    '	_ESCAPE	reduce 15',
    '	_EXCEPT	reduce 15',
    '	_FOR	reduce 15',
    '	_FROM	reduce 15',
    '	_FULL	reduce 15',
    '	_GRANT	reduce 15',
    '	_GROUP	reduce 15',
    '	_HAVING	reduce 15',
    '	_HOUR	reduce 15',
    '	_IN	reduce 15',
    '	_INNER	reduce 15',
    '	_INSERT	reduce 15',
    '	_INTERSECT	reduce 15',
    '	_INTO	reduce 15',
    '	_IS	reduce 15',
    '	_JOIN	reduce 15',
    '	_LEFT	reduce 15',
    '	_LIKE	reduce 15',
    '	_MATCH	reduce 15',
    '	_MINUTE	reduce 15',
    '	_MONTH	reduce 15',
    '	_NATURAL	reduce 15',
    '	_NOT	reduce 15',
    '	_OR	reduce 15',
    '	_ORDER	reduce 15',
    '	_OVERLAPS	reduce 15',
    '	_PRIMARY	reduce 15',
    '	_REFERENCES	reduce 15',
    '	_REVOKE	reduce 15',
    '	_RIGHT	reduce 15',
    '	_ROLLBACK	reduce 15',
    '	_SECOND	reduce 15',
    '	_SELECT	reduce 15',
    '	_SET	reduce 15',
    '	_TABLE	reduce 15',
    '	_THEN	reduce 15',
    '	_UNION	reduce 15',
    '	_UNIQUE	reduce 15',
    '	_UPDATE	reduce 15',
    '	_USER	reduce 15',
    '	_USING	reduce 15',
    '	_VALUES	reduce 15',
    '	_WHEN	reduce 15',
    '	_WHERE	reduce 15',
    '	_WITH	reduce 15',
    '	_YEAR	reduce 15',
    '	.	error',
    '',
    'state 521:',
    '',
    '	exact_numeric_literal_opt : period unsigned_integer _	(8)',
    '	unsigned_integer : unsigned_integer _ digit',
    '',
    '	digit	shift 331',
    '	$end	reduce 8',
    '	identifier_body	reduce 8',
    '	delimited_identifier	reduce 8',
    '	not_equals_operator	reduce 8',
    '	greater_than_or_equals_operator	reduce 8',
    '	less_than_or_equals_operator	reduce 8',
    '	concatenation_operator	reduce 8',
    '	left_paren	reduce 8',
    '	right_paren	reduce 8',
    '	asterisk	reduce 8',
    '	plus_sign	reduce 8',
    '	comma	reduce 8',
    '	minus_sign	reduce 8',
    '	solidus	reduce 8',
    '	semicolon	reduce 8',
    '	less_than_operator	reduce 8',
    '	equals_operator	reduce 8',
    '	greater_than_operator	reduce 8',
    '	underscore	reduce 8',
    '	_ALTER	reduce 8',
    '	_AND	reduce 8',
    '	_AS	reduce 8',
    '	_AT	reduce 8',
    '	_BETWEEN	reduce 8',
    '	_CHECK	reduce 8',
    '	_COLLATE	reduce 8',
    '	_COMMIT	reduce 8',
    '	_CONNECT	reduce 8',
    '	_CONSTRAINT	reduce 8',
    '	_CREATE	reduce 8',
    '	_CROSS	reduce 8',
    '	_DAY	reduce 8',
    '	_DECLARE	reduce 8',
    '	_DELETE	reduce 8',
    '	_DISCONNECT	reduce 8',
    '	_DROP	reduce 8',
    '	_ELSE	reduce 8',
    '	_END	reduce 8',
    '	_ESCAPE	reduce 8',
    '	_EXCEPT	reduce 8',
    '	_FOR	reduce 8',
    '	_FROM	reduce 8',
    '	_FULL	reduce 8',
    '	_GRANT	reduce 8',
    '	_GROUP	reduce 8',
    '	_HAVING	reduce 8',
    '	_HOUR	reduce 8',
    '	_IN	reduce 8',
    '	_INNER	reduce 8',
    '	_INSERT	reduce 8',
    '	_INTERSECT	reduce 8',
    '	_INTO	reduce 8',
    '	_IS	reduce 8',
    '	_JOIN	reduce 8',
    '	_LEFT	reduce 8',
    '	_LIKE	reduce 8',
    '	_MATCH	reduce 8',
    '	_MINUTE	reduce 8',
    '	_MONTH	reduce 8',
    '	_NATURAL	reduce 8',
    '	_NOT	reduce 8',
    '	_OR	reduce 8',
    '	_ORDER	reduce 8',
    '	_OVERLAPS	reduce 8',
    '	_PRIMARY	reduce 8',
    '	_REFERENCES	reduce 8',
    '	_REVOKE	reduce 8',
    '	_RIGHT	reduce 8',
    '	_ROLLBACK	reduce 8',
    '	_SECOND	reduce 8',
    '	_SELECT	reduce 8',
    '	_SET	reduce 8',
    '	_TABLE	reduce 8',
    '	_THEN	reduce 8',
    '	_UNION	reduce 8',
    '	_UNIQUE	reduce 8',
    '	_UPDATE	reduce 8',
    '	_USER	reduce 8',
    '	_USING	reduce 8',
    '	_VALUES	reduce 8',
    '	_WHEN	reduce 8',
    '	_WHERE	reduce 8',
    '	_WITH	reduce 8',
    '	_YEAR	reduce 8',
    '	_E	reduce 8',
    '	.	error',
    '',
    'state 522:',
    '',
    '	national_character_string_literal_cont : national_character_string_literal_cont string_literal_continuation _	(20)',
    '',
    '	.	reduce 20',
    '',
    'state 523:',
    '',
    '	bit_string_literal_cont : bit_string_literal_cont string_literal_continuation _	(23)',
    '',
    '	.	reduce 23',
    '',
    'state 524:',
    '',
    '	hex_string_literal_cont : hex_string_literal_cont string_literal_continuation _	(26)',
    '',
    '	.	reduce 26',
    '',
    'state 525:',
    '',
    '	date_string : quote date_value _ quote',
    '',
    '	quote	shift 768',
    '	.	error',
    '',
    'state 526:',
    '',
    '	unsigned_integer : unsigned_integer _ digit',
    '	date_value : unsigned_integer _ minus_sign unsigned_integer minus_sign unsigned_integer',
    '',
    '	digit	shift 331',
    '	minus_sign	shift 769',
    '	.	error',
    '',
    'state 527:',
    '',
    '	interval_literal : _INTERVAL interval_string interval_qualifier _	(217)',
    '',
    '	.	reduce 217',
    '',
    'state 528:',
    '',
    '	interval_literal : _INTERVAL sign interval_string _ interval_qualifier',
    '',
    '	_DAY	shift 415',
    '	_HOUR	shift 416',
    '	_MINUTE	shift 417',
    '	_MONTH	shift 418',
    '	_SECOND	shift 419',
    '	_YEAR	shift 420',
    '	.	error',
    '',
    '	non_second_datetime_field	goto 409',
    '	start_field	goto 410',
    '	interval_qualifier	goto 770',
    '',
    'state 529:',
    '',
    '	interval_string : quote interval_string_literal _ quote',
    '',
    '	quote	shift 771',
    '	.	error',
    '',
    'state 530:',
    '',
    '	unsigned_integer : unsigned_integer _ digit',
    '	interval_string_literal : unsigned_integer _	(53)',
    '	interval_string_literal : unsigned_integer _ minus_sign unsigned_integer',
    '	interval_string_literal : unsigned_integer _ space unsigned_integer',
    '	interval_string_literal : unsigned_integer _ space unsigned_integer colon unsigned_integer',
    '	interval_string_literal : unsigned_integer _ space unsigned_integer colon unsigned_integer colon seconds_value',
    '	interval_string_literal : unsigned_integer _ period unsigned_integer',
    '	interval_string_literal : unsigned_integer _ colon seconds_value',
    '	interval_string_literal : unsigned_integer _ colon unsigned_integer colon seconds_value',
    '',
    '	digit	shift 331',
    '	space	shift 772',
    '	minus_sign	shift 773',
    '	period	shift 774',
    '	colon	shift 775',
    '	quote	reduce 53',
    '	.	error',
    '',
    'state 531:',
    '',
    '	time_string : quote time_value _ quote quote time_value time_zone_interval quote',
    '',
    '	quote	shift 776',
    '	.	error',
    '',
    'state 532:',
    '',
    '	unsigned_integer : unsigned_integer _ digit',
    '	time_value : unsigned_integer _ colon unsigned_integer colon seconds_value',
    '',
    '	digit	shift 331',
    '	colon	shift 777',
    '	.	error',
    '',
    'state 533:',
    '',
    '	timestamp_string : quote date_value _ space time_value quote',
    '	timestamp_string : quote date_value _ space time_value time_zone_interval quote',
    '',
    '	space	shift 778',
    '	.	error',
    '',
    'state 534:',
    '',
    '	drop_character_set_statement : _DROP _CHARACTER _SET character_set_name _	(774)',
    '',
    '	.	reduce 774',
    '',
    'state 535:',
    '',
    '	drop_domain_statement : _DROP _DOMAIN domain_name drop_behaviour _	(773)',
    '',
    '	.	reduce 773',
    '',
    'state 536:',
    '',
    '	drop_behaviour : _CASCADE _	(740)',
    '',
    '	.	reduce 740',
    '',
    'state 537:',
    '',
    '	drop_behaviour : _RESTRICT _	(741)',
    '',
    '	.	reduce 741',
    '',
    'state 538:',
    '',
    '	drop_schema_statement : _DROP _SCHEMA schema_name drop_behaviour _	(739)',
    '',
    '	.	reduce 739',
    '',
    'state 539:',
    '',
    '	drop_table_statement : _DROP _TABLE table_name drop_behaviour _	(759)',
    '',
    '	.	reduce 759',
    '',
    'state 540:',
    '',
    '	drop_view_statement : _DROP _VIEW table_name drop_behaviour _	(760)',
    '',
    '	.	reduce 760',
    '',
    'state 541:',
    '',
    '	action_list : action_list comma action _	(674)',
    '',
    '	.	reduce 674',
    '',
    'state 542:',
    '',
    '	object_name : table_opt _ table_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	table_name	goto 779',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 543:',
    '',
    '	grant_statement : _GRANT privileges _ON object_name _ _TO grantee_list grant_option',
    '',
    '	_TO	shift 780',
    '	.	error',
    '',
    'state 544:',
    '',
    '	object_name : _CHARACTER _ _SET character_set_name',
    '',
    '	_SET	shift 781',
    '	.	error',
    '',
    'state 545:',
    '',
    '	object_name : _COLLATION _ collation_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	collation_name	goto 782',
    '	qualified_name	goto 313',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 546:',
    '',
    '	object_name : _DOMAIN _ domain_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	qualified_name	goto 301',
    '	domain_name	goto 783',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 547:',
    '',
    '	table_opt : _TABLE _	(690)',
    '',
    '	.	reduce 690',
    '',
    'state 548:',
    '',
    '	object_name : _TRANSLATION _ translation_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	translation_name	goto 784',
    '	qualified_name	goto 322',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 549:',
    '',
    '	privilege_column_list_opt : left_paren privilege_column_list _ right_paren',
    '',
    '	right_paren	shift 785',
    '	.	error',
    '',
    'state 550:',
    '',
    '	column_name_list : column_name_list _ comma column_name',
    '	privilege_column_list : column_name_list _	(683)',
    '',
    '	comma	shift 786',
    '	right_paren	reduce 683',
    '	.	error',
    '',
    'state 551:',
    '',
    '	column_name_list : column_name _	(249)',
    '',
    '	.	reduce 249',
    '',
    'state 552:',
    '',
    '	insert_statement : _INSERT _INTO table_name insert_columns_and_source _	(810)',
    '',
    '	.	reduce 810',
    '',
    'state 553:',
    '',
    '	query_expression : query_expression _ _UNION all_opt corresponding_spec_opt query_term',
    '	query_expression : query_expression _ _EXCEPT all_opt corresponding_spec_opt query_term',
    '	insert_columns_and_source : query_expression _	(812)',
    '',
    '	_EXCEPT	shift 91',
    '	_UNION	shift 93',
    '	$end	reduce 812',
    '	identifier_body	reduce 812',
    '	delimited_identifier	reduce 812',
    '	left_paren	reduce 812',
    '	semicolon	reduce 812',
    '	underscore	reduce 812',
    '	_ALTER	reduce 812',
    '	_COMMIT	reduce 812',
    '	_CONNECT	reduce 812',
    '	_CREATE	reduce 812',
    '	_DECLARE	reduce 812',
    '	_DELETE	reduce 812',
    '	_DISCONNECT	reduce 812',
    '	_DROP	reduce 812',
    '	_GRANT	reduce 812',
    '	_INSERT	reduce 812',
    '	_REVOKE	reduce 812',
    '	_ROLLBACK	reduce 812',
    '	_SELECT	reduce 812',
    '	_SET	reduce 812',
    '	_TABLE	reduce 812',
    '	_UPDATE	reduce 812',
    '	_VALUES	reduce 812',
    '	.	error',
    '',
    'state 554:',
    '',
    '	table_subquery : left_paren _ query_expression right_paren',
    '	insert_columns_and_source : left_paren _ insert_column_list right_paren query_expression',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	left_paren	shift 68',
    '	underscore	shift 69',
    '	_SELECT	shift 83',
    '	_TABLE	shift 85',
    '	_VALUES	shift 87',
    '	.	error',
    '',
    '	insert_column_list	goto 787',
    '	explicit_table	goto 49',
    '	table_value_constructor	goto 50',
    '	query_specification	goto 51',
    '	table_subquery	goto 52',
    '	simple_table	goto 53',
    '	non_join_query_primary	goto 54',
    '	query_term	goto 55',
    '	non_join_query_term	goto 56',
    '	query_expression	goto 101',
    '	column_name_list	goto 788',
    '	column_name	goto 551',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 555:',
    '',
    '	insert_columns_and_source : _DEFAULT _ _VALUES',
    '',
    '	_VALUES	shift 789',
    '	.	error',
    '',
    'state 556:',
    '',
    '	module_name_clause : _MODULE _MODULE module_name _MODULE _ module_character_set_specification _MODULE module_name module_character_set_specification',
    '',
    '	_NAMES	shift 791',
    '	.	error',
    '',
    '	module_character_set_specification	goto 790',
    '',
    'state 557:',
    '',
    '	revoke_statement : _REVOKE grant_option_for_opt privileges _ON _ object_name _FROM grantee_list drop_behaviour',
    '	table_opt : _	(689)',
    '',
    '	_CHARACTER	shift 544',
    '	_COLLATION	shift 545',
    '	_DOMAIN	shift 546',
    '	_TABLE	shift 547',
    '	_TRANSLATION	shift 548',
    '	identifier_body	reduce 689',
    '	delimited_identifier	reduce 689',
    '	underscore	reduce 689',
    '	_MODULE	reduce 689',
    '	.	error',
    '',
    '	table_opt	goto 542',
    '	object_name	goto 792',
    '',
    'state 558:',
    '',
    '	grant_option_for_opt : _GRANT _OPTION _FOR _	(763)',
    '',
    '	.	reduce 763',
    '',
    'state 559:',
    '',
    '	select_list_opt : select_list_opt comma _ select_sublist',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	derived_column	goto 367',
    '	select_sublist	goto 793',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 371',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name_trail_asterisk	goto 372',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 373',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 560:',
    '',
    '	table_expression : from_clause _ where_clause_opt group_by_clause_opt having_clause_opt',
    '	where_clause_opt : _	(377)',
    '',
    '	_WHERE	shift 515',
    '	$end	reduce 377',
    '	identifier_body	reduce 377',
    '	delimited_identifier	reduce 377',
    '	left_paren	reduce 377',
    '	right_paren	reduce 377',
    '	semicolon	reduce 377',
    '	underscore	reduce 377',
    '	_ALTER	reduce 377',
    '	_COMMIT	reduce 377',
    '	_CONNECT	reduce 377',
    '	_CREATE	reduce 377',
    '	_DECLARE	reduce 377',
    '	_DELETE	reduce 377',
    '	_DISCONNECT	reduce 377',
    '	_DROP	reduce 377',
    '	_EXCEPT	reduce 377',
    '	_FOR	reduce 377',
    '	_GRANT	reduce 377',
    '	_GROUP	reduce 377',
    '	_HAVING	reduce 377',
    '	_INSERT	reduce 377',
    '	_INTERSECT	reduce 377',
    '	_ORDER	reduce 377',
    '	_REVOKE	reduce 377',
    '	_ROLLBACK	reduce 377',
    '	_SELECT	reduce 377',
    '	_SET	reduce 377',
    '	_TABLE	reduce 377',
    '	_UNION	reduce 377',
    '	_UPDATE	reduce 377',
    '	_VALUES	reduce 377',
    '	_WITH	reduce 377',
    '	.	error',
    '',
    '	where_clause	goto 513',
    '	where_clause_opt	goto 794',
    '',
    'state 561:',
    '',
    '	query_specification : _SELECT set_quantifier_opt select_list table_expression _	(365)',
    '',
    '	.	reduce 365',
    '',
    'state 562:',
    '',
    '	from_clause : _FROM _ from_clause_opt',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	left_paren	shift 804',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	qualified_join	goto 795',
    '	cross_join	goto 796',
    '	derived_table	goto 797',
    '	table_factor	goto 798',
    '	joined_table	goto 799',
    '	table_reference	goto 800',
    '	from_clause_opt	goto 801',
    '	table_subquery	goto 802',
    '	table_name	goto 803',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 563:',
    '',
    '	derived_column : expression as_clause _	(373)',
    '',
    '	.	reduce 373',
    '',
    'state 564:',
    '',
    '	as_clause : column_name _	(374)',
    '',
    '	.	reduce 374',
    '',
    'state 565:',
    '',
    '	as_clause : _AS _ column_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	column_name	goto 805',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 566:',
    '',
    '	qualified_name : identifier period _ identifier',
    '	qualified_name : identifier period _ identifier period identifier',
    '	qualified_name_trail_asterisk : identifier period _ asterisk',
    '	qualified_name_trail_asterisk : identifier period _ identifier period asterisk',
    '	qualified_name_trail_asterisk : identifier period _ identifier period identifier period asterisk',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	asterisk	shift 807',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	actual_identifier	goto 61',
    '	identifier	goto 806',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 567:',
    '',
    '	primary_expression : left_paren expression _ right_paren',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '',
    '	concatenation_operator	shift 421',
    '	right_paren	shift 609',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	.	error',
    '',
    'state 568:',
    '',
    '	constraint_name_list_some : constraint_name_list_some comma _ constraint_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	constraint_name	goto 808',
    '	qualified_name	goto 310',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 569:',
    '',
    '	set_constraints_mode_statement : _SET _CONSTRAINTS constraint_name_list _DEFERRED _	(842)',
    '',
    '	.	reduce 842',
    '',
    'state 570:',
    '',
    '	set_constraints_mode_statement : _SET _CONSTRAINTS constraint_name_list _IMMEDIATE _	(843)',
    '',
    '	.	reduce 843',
    '',
    'state 571:',
    '',
    '	set_session_authorization_identifier_statement : _SET _SESSION _AUTHORIZATION value_specification _	(882)',
    '',
    '	.	reduce 882',
    '',
    'state 572:',
    '',
    '	set_local_time_zone_statement : _SET _TIME _ZONE set_time_zone_value _	(883)',
    '',
    '	.	reduce 883',
    '',
    'state 573:',
    '',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	set_time_zone_value : expression _	(884)',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	$end	reduce 884',
    '	identifier_body	reduce 884',
    '	delimited_identifier	reduce 884',
    '	left_paren	reduce 884',
    '	semicolon	reduce 884',
    '	underscore	reduce 884',
    '	_ALTER	reduce 884',
    '	_COMMIT	reduce 884',
    '	_CONNECT	reduce 884',
    '	_CREATE	reduce 884',
    '	_DECLARE	reduce 884',
    '	_DELETE	reduce 884',
    '	_DISCONNECT	reduce 884',
    '	_DROP	reduce 884',
    '	_GRANT	reduce 884',
    '	_INSERT	reduce 884',
    '	_REVOKE	reduce 884',
    '	_ROLLBACK	reduce 884',
    '	_SELECT	reduce 884',
    '	_SET	reduce 884',
    '	_TABLE	reduce 884',
    '	_UPDATE	reduce 884',
    '	_VALUES	reduce 884',
    '	.	error',
    '',
    'state 574:',
    '',
    '	set_time_zone_value : _LOCAL _	(885)',
    '',
    '	.	reduce 885',
    '',
    'state 575:',
    '',
    '	transaction_mode_list : transaction_mode_list comma _ transaction_mode',
    '',
    '	_DIAGNOSTICS	shift 393',
    '	_ISOLATION	shift 394',
    '	_READ	shift 395',
    '	.	error',
    '',
    '	diagnostics_size	goto 388',
    '	transaction_access_mode	goto 389',
    '	isolation_level	goto 390',
    '	transaction_mode	goto 809',
    '',
    'state 576:',
    '',
    '	diagnostics_size : _DIAGNOSTICS _SIZE _ number_of_conditions',
    '',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	digit	shift 147',
    '	plus_sign	shift 148',
    '	minus_sign	shift 149',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_DATE	shift 154',
    '	_INTERVAL	shift 156',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	.	error',
    '',
    '	number_of_conditions	goto 810',
    '	simple_value_specification	goto 811',
    '	parameter_name	goto 122',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 128',
    '	signed_numeric_literal	goto 129',
    '	literal	goto 130',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 132',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	sign	goto 137',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 142',
    '',
    'state 577:',
    '',
    '	isolation_level : _ISOLATION _LEVEL _ level_of_isolation',
    '',
    '	_READ	shift 813',
    '	_REPEATABLE	shift 814',
    '	_SERIALIZABLE	shift 815',
    '	_SNAPSHOT	shift 816',
    '	.	error',
    '',
    '	level_of_isolation	goto 812',
    '',
    'state 578:',
    '',
    '	transaction_access_mode : _READ _ONLY _	(838)',
    '',
    '	.	reduce 838',
    '',
    'state 579:',
    '',
    '	transaction_access_mode : _READ _WRITE _	(839)',
    '',
    '	.	reduce 839',
    '',
    'state 580:',
    '',
    '	qualified_name : identifier period identifier _	(188)',
    '	qualified_name : identifier period identifier _ period identifier',
    '',
    '	period	shift 817',
    '	$end	reduce 188',
    '	identifier_body	reduce 188',
    '	delimited_identifier	reduce 188',
    '	not_equals_operator	reduce 188',
    '	greater_than_or_equals_operator	reduce 188',
    '	less_than_or_equals_operator	reduce 188',
    '	concatenation_operator	reduce 188',
    '	quote	reduce 188',
    '	left_paren	reduce 188',
    '	right_paren	reduce 188',
    '	asterisk	reduce 188',
    '	plus_sign	reduce 188',
    '	comma	reduce 188',
    '	minus_sign	reduce 188',
    '	solidus	reduce 188',
    '	semicolon	reduce 188',
    '	less_than_operator	reduce 188',
    '	equals_operator	reduce 188',
    '	greater_than_operator	reduce 188',
    '	underscore	reduce 188',
    '	_ADD	reduce 188',
    '	_ALTER	reduce 188',
    '	_AND	reduce 188',
    '	_AS	reduce 188',
    '	_ASC	reduce 188',
    '	_AT	reduce 188',
    '	_BETWEEN	reduce 188',
    '	_BIT	reduce 188',
    '	_CASCADE	reduce 188',
    '	_CHAR	reduce 188',
    '	_CHARACTER	reduce 188',
    '	_CHECK	reduce 188',
    '	_COLLATE	reduce 188',
    '	_COMMIT	reduce 188',
    '	_CONNECT	reduce 188',
    '	_CONSTRAINT	reduce 188',
    '	_CREATE	reduce 188',
    '	_CROSS	reduce 188',
    '	_DATE	reduce 188',
    '	_DAY	reduce 188',
    '	_DEC	reduce 188',
    '	_DECIMAL	reduce 188',
    '	_DECLARE	reduce 188',
    '	_DEFAULT	reduce 188',
    '	_DEFERRABLE	reduce 188',
    '	_DEFERRED	reduce 188',
    '	_DELETE	reduce 188',
    '	_DESC	reduce 188',
    '	_DISCONNECT	reduce 188',
    '	_DOUBLE	reduce 188',
    '	_DROP	reduce 188',
    '	_ELSE	reduce 188',
    '	_END	reduce 188',
    '	_ESCAPE	reduce 188',
    '	_EXCEPT	reduce 188',
    '	_FLOAT	reduce 188',
    '	_FOR	reduce 188',
    '	_FOREIGN	reduce 188',
    '	_FROM	reduce 188',
    '	_FULL	reduce 188',
    '	_GRANT	reduce 188',
    '	_GROUP	reduce 188',
    '	_HAVING	reduce 188',
    '	_HOUR	reduce 188',
    '	_IMMEDIATE	reduce 188',
    '	_IN	reduce 188',
    '	_INITIALLY	reduce 188',
    '	_INNER	reduce 188',
    '	_INSERT	reduce 188',
    '	_INT	reduce 188',
    '	_INTEGER	reduce 188',
    '	_INTERSECT	reduce 188',
    '	_INTERVAL	reduce 188',
    '	_INTO	reduce 188',
    '	_IS	reduce 188',
    '	_JOIN	reduce 188',
    '	_LEFT	reduce 188',
    '	_LIKE	reduce 188',
    '	_MATCH	reduce 188',
    '	_MINUTE	reduce 188',
    '	_MONTH	reduce 188',
    '	_NATIONAL	reduce 188',
    '	_NATURAL	reduce 188',
    '	_NCHAR	reduce 188',
    '	_NO	reduce 188',
    '	_NOT	reduce 188',
    '	_NUMERIC	reduce 188',
    '	_ON	reduce 188',
    '	_OR	reduce 188',
    '	_ORDER	reduce 188',
    '	_OVERLAPS	reduce 188',
    '	_PAD	reduce 188',
    '	_PRIMARY	reduce 188',
    '	_REAL	reduce 188',
    '	_REFERENCES	reduce 188',
    '	_RESTRICT	reduce 188',
    '	_REVOKE	reduce 188',
    '	_RIGHT	reduce 188',
    '	_ROLLBACK	reduce 188',
    '	_SECOND	reduce 188',
    '	_SELECT	reduce 188',
    '	_SET	reduce 188',
    '	_SMALLINT	reduce 188',
    '	_TABLE	reduce 188',
    '	_THEN	reduce 188',
    '	_TIME	reduce 188',
    '	_TIMESTAMP	reduce 188',
    '	_TO	reduce 188',
    '	_UNION	reduce 188',
    '	_UNIQUE	reduce 188',
    '	_UPDATE	reduce 188',
    '	_USING	reduce 188',
    '	_VALUES	reduce 188',
    '	_VARCHAR	reduce 188',
    '	_WHEN	reduce 188',
    '	_WHERE	reduce 188',
    '	_WITH	reduce 188',
    '	_YEAR	reduce 188',
    '	.	error',
    '',
    'state 581:',
    '',
    '	qualified_local_table_name : _MODULE period local_table_name _	(84)',
    '',
    '	.	reduce 84',
    '',
    'state 582:',
    '',
    '	local_table_name : identifier _	(85)',
    '',
    '	.	reduce 85',
    '',
    'state 583:',
    '',
    '	set_clause : object_column _ equals_operator update_source',
    '',
    '	equals_operator	shift 818',
    '	.	error',
    '',
    'state 584:',
    '',
    '	set_clause_list : set_clause _	(816)',
    '',
    '	.	reduce 816',
    '',
    'state 585:',
    '',
    '	update_statement__searched : _UPDATE table_name _SET set_clause_list _ where_clause_opt',
    '	set_clause_list : set_clause_list _ comma set_clause',
    '	where_clause_opt : _	(377)',
    '',
    '	comma	shift 820',
    '	_WHERE	shift 515',
    '	$end	reduce 377',
    '	identifier_body	reduce 377',
    '	delimited_identifier	reduce 377',
    '	left_paren	reduce 377',
    '	underscore	reduce 377',
    '	_ALTER	reduce 377',
    '	_COMMIT	reduce 377',
    '	_CONNECT	reduce 377',
    '	_CREATE	reduce 377',
    '	_DECLARE	reduce 377',
    '	_DELETE	reduce 377',
    '	_DISCONNECT	reduce 377',
    '	_DROP	reduce 377',
    '	_GRANT	reduce 377',
    '	_INSERT	reduce 377',
    '	_REVOKE	reduce 377',
    '	_ROLLBACK	reduce 377',
    '	_SELECT	reduce 377',
    '	_SET	reduce 377',
    '	_TABLE	reduce 377',
    '	_UPDATE	reduce 377',
    '	_VALUES	reduce 377',
    '	.	error',
    '',
    '	where_clause	goto 513',
    '	where_clause_opt	goto 819',
    '',
    'state 586:',
    '',
    '	object_column : column_name _	(819)',
    '',
    '	.	reduce 819',
    '',
    'state 587:',
    '',
    '	char_length_expression : char_length_specifier left_paren expression _ right_paren',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '',
    '	concatenation_operator	shift 421',
    '	right_paren	shift 821',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	.	error',
    '',
    'state 588:',
    '',
    '	table_value_constructor_list : table_value_constructor_list comma row_value_constructor _	(433)',
    '',
    '	.	reduce 433',
    '',
    'state 589:',
    '',
    '	set_quantifier_args : set_quantifier _ expression',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 822',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 590:',
    '',
    '	general_set_function : set_function_type left_paren set_quantifier_args _ right_paren',
    '',
    '	right_paren	shift 823',
    '	.	error',
    '',
    'state 591:',
    '',
    '	set_quantifier_args : asterisk _	(340)',
    '',
    '	.	reduce 340',
    '',
    'state 592:',
    '',
    '	indicator_parameter_opt : _INDICATOR parameter_name _	(333)',
    '',
    '	.	reduce 333',
    '',
    'state 593:',
    '',
    '	multiplicative_expression : multiplicative_expression asterisk unary_expression _	(311)',
    '',
    '	.	reduce 311',
    '',
    'state 594:',
    '',
    '	multiplicative_expression : multiplicative_expression solidus unary_expression _	(312)',
    '',
    '	.	reduce 312',
    '',
    'state 595:',
    '',
    '	start_field : non_second_datetime_field left_paren _ precision right_paren',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	precision	goto 824',
    '	unsigned_integer	goto 825',
    '',
    'state 596:',
    '',
    '	interval_qualifier : start_field _TO _ end_field',
    '',
    '	_DAY	shift 415',
    '	_HOUR	shift 416',
    '	_MINUTE	shift 417',
    '	_MONTH	shift 418',
    '	_SECOND	shift 828',
    '	_YEAR	shift 420',
    '	.	error',
    '',
    '	non_second_datetime_field	goto 826',
    '	end_field	goto 827',
    '',
    'state 597:',
    '',
    '	time_zone : _AT time_zone_specifier _	(501)',
    '',
    '	.	reduce 501',
    '',
    'state 598:',
    '',
    '	time_zone_specifier : _LOCAL _	(502)',
    '',
    '	.	reduce 502',
    '',
    'state 599:',
    '',
    '	time_zone_specifier : _TIME _ _ZONE expression',
    '',
    '	_ZONE	shift 829',
    '	.	error',
    '',
    'state 600:',
    '',
    '	collate_clause : _COLLATE collation_name _	(428)',
    '',
    '	.	reduce 428',
    '',
    'state 601:',
    '',
    '	interval_qualifier : _SECOND single_datetime_field_opt _	(169)',
    '',
    '	.	reduce 169',
    '',
    'state 602:',
    '',
    '	single_datetime_field_opt : left_paren _ interval_leading_field_precision single_datetime_field_opt2 right_paren',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	interval_leading_field_precision	goto 830',
    '	unsigned_integer	goto 831',
    '',
    'state 603:',
    '',
    '	expression : expression concatenation_operator multiplicative_expression _	(316)',
    '	multiplicative_expression : multiplicative_expression _ asterisk unary_expression',
    '	multiplicative_expression : multiplicative_expression _ solidus unary_expression',
    '',
    '	asterisk	shift 405',
    '	solidus	shift 406',
    '	$end	reduce 316',
    '	identifier_body	reduce 316',
    '	delimited_identifier	reduce 316',
    '	not_equals_operator	reduce 316',
    '	greater_than_or_equals_operator	reduce 316',
    '	less_than_or_equals_operator	reduce 316',
    '	concatenation_operator	reduce 316',
    '	left_paren	reduce 316',
    '	right_paren	reduce 316',
    '	plus_sign	reduce 316',
    '	comma	reduce 316',
    '	minus_sign	reduce 316',
    '	semicolon	reduce 316',
    '	less_than_operator	reduce 316',
    '	equals_operator	reduce 316',
    '	greater_than_operator	reduce 316',
    '	underscore	reduce 316',
    '	_ALTER	reduce 316',
    '	_AND	reduce 316',
    '	_AS	reduce 316',
    '	_BETWEEN	reduce 316',
    '	_COMMIT	reduce 316',
    '	_CONNECT	reduce 316',
    '	_CREATE	reduce 316',
    '	_CROSS	reduce 316',
    '	_DECLARE	reduce 316',
    '	_DELETE	reduce 316',
    '	_DISCONNECT	reduce 316',
    '	_DROP	reduce 316',
    '	_ELSE	reduce 316',
    '	_END	reduce 316',
    '	_ESCAPE	reduce 316',
    '	_EXCEPT	reduce 316',
    '	_FOR	reduce 316',
    '	_FROM	reduce 316',
    '	_FULL	reduce 316',
    '	_GRANT	reduce 316',
    '	_GROUP	reduce 316',
    '	_HAVING	reduce 316',
    '	_IN	reduce 316',
    '	_INNER	reduce 316',
    '	_INSERT	reduce 316',
    '	_INTERSECT	reduce 316',
    '	_INTO	reduce 316',
    '	_IS	reduce 316',
    '	_JOIN	reduce 316',
    '	_LEFT	reduce 316',
    '	_LIKE	reduce 316',
    '	_MATCH	reduce 316',
    '	_NATURAL	reduce 316',
    '	_NOT	reduce 316',
    '	_OR	reduce 316',
    '	_ORDER	reduce 316',
    '	_OVERLAPS	reduce 316',
    '	_REVOKE	reduce 316',
    '	_RIGHT	reduce 316',
    '	_ROLLBACK	reduce 316',
    '	_SELECT	reduce 316',
    '	_SET	reduce 316',
    '	_TABLE	reduce 316',
    '	_THEN	reduce 316',
    '	_UNION	reduce 316',
    '	_UPDATE	reduce 316',
    '	_USING	reduce 316',
    '	_VALUES	reduce 316',
    '	_WHEN	reduce 316',
    '	_WHERE	reduce 316',
    '	_WITH	reduce 316',
    '	.	error',
    '',
    'state 604:',
    '',
    '	expression : expression plus_sign multiplicative_expression _	(314)',
    '	multiplicative_expression : multiplicative_expression _ asterisk unary_expression',
    '	multiplicative_expression : multiplicative_expression _ solidus unary_expression',
    '',
    '	asterisk	shift 405',
    '	solidus	shift 406',
    '	$end	reduce 314',
    '	identifier_body	reduce 314',
    '	delimited_identifier	reduce 314',
    '	not_equals_operator	reduce 314',
    '	greater_than_or_equals_operator	reduce 314',
    '	less_than_or_equals_operator	reduce 314',
    '	concatenation_operator	reduce 314',
    '	left_paren	reduce 314',
    '	right_paren	reduce 314',
    '	plus_sign	reduce 314',
    '	comma	reduce 314',
    '	minus_sign	reduce 314',
    '	semicolon	reduce 314',
    '	less_than_operator	reduce 314',
    '	equals_operator	reduce 314',
    '	greater_than_operator	reduce 314',
    '	underscore	reduce 314',
    '	_ALTER	reduce 314',
    '	_AND	reduce 314',
    '	_AS	reduce 314',
    '	_BETWEEN	reduce 314',
    '	_COMMIT	reduce 314',
    '	_CONNECT	reduce 314',
    '	_CREATE	reduce 314',
    '	_CROSS	reduce 314',
    '	_DECLARE	reduce 314',
    '	_DELETE	reduce 314',
    '	_DISCONNECT	reduce 314',
    '	_DROP	reduce 314',
    '	_ELSE	reduce 314',
    '	_END	reduce 314',
    '	_ESCAPE	reduce 314',
    '	_EXCEPT	reduce 314',
    '	_FOR	reduce 314',
    '	_FROM	reduce 314',
    '	_FULL	reduce 314',
    '	_GRANT	reduce 314',
    '	_GROUP	reduce 314',
    '	_HAVING	reduce 314',
    '	_IN	reduce 314',
    '	_INNER	reduce 314',
    '	_INSERT	reduce 314',
    '	_INTERSECT	reduce 314',
    '	_INTO	reduce 314',
    '	_IS	reduce 314',
    '	_JOIN	reduce 314',
    '	_LEFT	reduce 314',
    '	_LIKE	reduce 314',
    '	_MATCH	reduce 314',
    '	_NATURAL	reduce 314',
    '	_NOT	reduce 314',
    '	_OR	reduce 314',
    '	_ORDER	reduce 314',
    '	_OVERLAPS	reduce 314',
    '	_REVOKE	reduce 314',
    '	_RIGHT	reduce 314',
    '	_ROLLBACK	reduce 314',
    '	_SELECT	reduce 314',
    '	_SET	reduce 314',
    '	_TABLE	reduce 314',
    '	_THEN	reduce 314',
    '	_UNION	reduce 314',
    '	_UPDATE	reduce 314',
    '	_USING	reduce 314',
    '	_VALUES	reduce 314',
    '	_WHEN	reduce 314',
    '	_WHERE	reduce 314',
    '	_WITH	reduce 314',
    '	.	error',
    '',
    'state 605:',
    '',
    '	expression : expression minus_sign multiplicative_expression _	(315)',
    '	multiplicative_expression : multiplicative_expression _ asterisk unary_expression',
    '	multiplicative_expression : multiplicative_expression _ solidus unary_expression',
    '',
    '	asterisk	shift 405',
    '	solidus	shift 406',
    '	$end	reduce 315',
    '	identifier_body	reduce 315',
    '	delimited_identifier	reduce 315',
    '	not_equals_operator	reduce 315',
    '	greater_than_or_equals_operator	reduce 315',
    '	less_than_or_equals_operator	reduce 315',
    '	concatenation_operator	reduce 315',
    '	left_paren	reduce 315',
    '	right_paren	reduce 315',
    '	plus_sign	reduce 315',
    '	comma	reduce 315',
    '	minus_sign	reduce 315',
    '	semicolon	reduce 315',
    '	less_than_operator	reduce 315',
    '	equals_operator	reduce 315',
    '	greater_than_operator	reduce 315',
    '	underscore	reduce 315',
    '	_ALTER	reduce 315',
    '	_AND	reduce 315',
    '	_AS	reduce 315',
    '	_BETWEEN	reduce 315',
    '	_COMMIT	reduce 315',
    '	_CONNECT	reduce 315',
    '	_CREATE	reduce 315',
    '	_CROSS	reduce 315',
    '	_DECLARE	reduce 315',
    '	_DELETE	reduce 315',
    '	_DISCONNECT	reduce 315',
    '	_DROP	reduce 315',
    '	_ELSE	reduce 315',
    '	_END	reduce 315',
    '	_ESCAPE	reduce 315',
    '	_EXCEPT	reduce 315',
    '	_FOR	reduce 315',
    '	_FROM	reduce 315',
    '	_FULL	reduce 315',
    '	_GRANT	reduce 315',
    '	_GROUP	reduce 315',
    '	_HAVING	reduce 315',
    '	_IN	reduce 315',
    '	_INNER	reduce 315',
    '	_INSERT	reduce 315',
    '	_INTERSECT	reduce 315',
    '	_INTO	reduce 315',
    '	_IS	reduce 315',
    '	_JOIN	reduce 315',
    '	_LEFT	reduce 315',
    '	_LIKE	reduce 315',
    '	_MATCH	reduce 315',
    '	_NATURAL	reduce 315',
    '	_NOT	reduce 315',
    '	_OR	reduce 315',
    '	_ORDER	reduce 315',
    '	_OVERLAPS	reduce 315',
    '	_REVOKE	reduce 315',
    '	_RIGHT	reduce 315',
    '	_ROLLBACK	reduce 315',
    '	_SELECT	reduce 315',
    '	_SET	reduce 315',
    '	_TABLE	reduce 315',
    '	_THEN	reduce 315',
    '	_UNION	reduce 315',
    '	_UPDATE	reduce 315',
    '	_USING	reduce 315',
    '	_VALUES	reduce 315',
    '	_WHEN	reduce 315',
    '	_WHERE	reduce 315',
    '	_WITH	reduce 315',
    '	.	error',
    '',
    'state 606:',
    '',
    '	scalar_subquery : left_paren subquery right_paren _	(318)',
    '',
    '	.	reduce 318',
    '',
    'state 607:',
    '',
    '	row_value_constructor : left_paren row_value_constructor_list right_paren _	(289)',
    '',
    '	.	reduce 289',
    '',
    'state 608:',
    '',
    '	row_value_constructor_list : row_value_constructor_list comma _ expression',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 832',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 609:',
    '',
    '	primary_expression : left_paren expression right_paren _	(301)',
    '',
    '	.	reduce 301',
    '',
    'state 610:',
    '',
    '	*** conflicts:',
    '',
    '	shift 300, reduce 319 on right_paren',
    '',
    '	table_subquery : left_paren query_expression _ right_paren',
    '	subquery : query_expression _	(319)',
    '	query_expression : query_expression _ _UNION all_opt corresponding_spec_opt query_term',
    '	query_expression : query_expression _ _EXCEPT all_opt corresponding_spec_opt query_term',
    '',
    '	right_paren	shift 300',
    '	_EXCEPT	shift 91',
    '	_UNION	shift 93',
    '	.	error',
    '',
    'state 611:',
    '',
    '	bit_length_expression : _BIT_LENGTH left_paren expression _ right_paren',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '',
    '	concatenation_operator	shift 421',
    '	right_paren	shift 833',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	.	error',
    '',
    'state 612:',
    '',
    '	else_clause_opt : else_clause _	(451)',
    '',
    '	.	reduce 451',
    '',
    'state 613:',
    '',
    '	searched_case : _CASE searched_when_clause else_clause_opt _ _END',
    '',
    '	_END	shift 834',
    '	.	error',
    '',
    'state 614:',
    '',
    '	else_clause : _ELSE _ result',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	result	goto 835',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 836',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 615:',
    '',
    '	simple_case : _CASE case_operand simple_when_clause _ else_clause_opt _END',
    '	else_clause_opt : _	(450)',
    '',
    '	_ELSE	shift 614',
    '	_END	reduce 450',
    '	.	error',
    '',
    '	else_clause	goto 612',
    '	else_clause_opt	goto 837',
    '',
    'state 616:',
    '',
    '	simple_when_clause : _WHEN _ when_operand _THEN result',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	when_operand	goto 838',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 839',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 617:',
    '',
    '	overlaps_predicate : row_value_constructor_1 _ _OVERLAPS row_value_constructor_2',
    '',
    '	_OVERLAPS	shift 840',
    '	.	error',
    '',
    'state 618:',
    '',
    '	*** conflicts:',
    '',
    '	shift 842, reduce 288 on _NOT',
    '',
    '	row_value_constructor : expression _	(288)',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	like_predicate : expression _ _LIKE pattern like_predicate_escape_opt',
    '	like_predicate : expression _ _NOT _LIKE pattern like_predicate_escape_opt',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	_LIKE	shift 841',
    '	_NOT	shift 842',
    '	not_equals_operator	reduce 288',
    '	greater_than_or_equals_operator	reduce 288',
    '	less_than_or_equals_operator	reduce 288',
    '	less_than_operator	reduce 288',
    '	equals_operator	reduce 288',
    '	greater_than_operator	reduce 288',
    '	_BETWEEN	reduce 288',
    '	_IN	reduce 288',
    '	_IS	reduce 288',
    '	_MATCH	reduce 288',
    '	_OVERLAPS	reduce 288',
    '	.	error',
    '',
    'state 619:',
    '',
    '	comparison_predicate : row_value_constructor _ comp_op row_value_constructor',
    '	between_predicate : row_value_constructor _ _BETWEEN row_value_constructor _AND row_value_constructor',
    '	between_predicate : row_value_constructor _ _NOT _BETWEEN row_value_constructor _AND row_value_constructor',
    '	in_predicate : row_value_constructor _ _IN in_predicate_value',
    '	in_predicate : row_value_constructor _ _NOT _IN in_predicate_value',
    '	null_predicate : row_value_constructor _ _IS _NULL',
    '	null_predicate : row_value_constructor _ _IS _NOT _NULL',
    '	quantified_comparison_predicate : row_value_constructor _ comp_op quantifier table_subquery',
    '	match_predicate : row_value_constructor _ _MATCH unique_opt partial_full_opt table_subquery',
    '	row_value_constructor_1 : row_value_constructor _	(553)',
    '',
    '	not_equals_operator	shift 844',
    '	greater_than_or_equals_operator	shift 845',
    '	less_than_or_equals_operator	shift 846',
    '	less_than_operator	shift 847',
    '	equals_operator	shift 848',
    '	greater_than_operator	shift 849',
    '	_BETWEEN	shift 850',
    '	_IN	shift 851',
    '	_IS	shift 852',
    '	_MATCH	shift 853',
    '	_NOT	shift 854',
    '	_OVERLAPS	reduce 553',
    '	.	error',
    '',
    '	comp_op	goto 843',
    '',
    'state 620:',
    '',
    '	predicate : overlaps_predicate _	(286)',
    '',
    '	.	reduce 286',
    '',
    'state 621:',
    '',
    '	predicate : match_predicate _	(285)',
    '',
    '	.	reduce 285',
    '',
    'state 622:',
    '',
    '	predicate : unique_predicate _	(284)',
    '',
    '	.	reduce 284',
    '',
    'state 623:',
    '',
    '	predicate : exists_predicate _	(283)',
    '',
    '	.	reduce 283',
    '',
    'state 624:',
    '',
    '	predicate : quantified_comparison_predicate _	(282)',
    '',
    '	.	reduce 282',
    '',
    'state 625:',
    '',
    '	predicate : null_predicate _	(281)',
    '',
    '	.	reduce 281',
    '',
    'state 626:',
    '',
    '	predicate : like_predicate _	(280)',
    '',
    '	.	reduce 280',
    '',
    'state 627:',
    '',
    '	predicate : in_predicate _	(279)',
    '',
    '	.	reduce 279',
    '',
    'state 628:',
    '',
    '	predicate : between_predicate _	(278)',
    '',
    '	.	reduce 278',
    '',
    'state 629:',
    '',
    '	predicate : comparison_predicate _	(277)',
    '',
    '	.	reduce 277',
    '',
    'state 630:',
    '',
    '	boolean_primary : predicate _	(275)',
    '',
    '	.	reduce 275',
    '',
    'state 631:',
    '',
    '	boolean_test : boolean_primary _	(272)',
    '	boolean_test : boolean_primary _ _IS truth_value',
    '	boolean_test : boolean_primary _ _IS _NOT truth_value',
    '',
    '	_IS	shift 855',
    '	$end	reduce 272',
    '	identifier_body	reduce 272',
    '	delimited_identifier	reduce 272',
    '	left_paren	reduce 272',
    '	right_paren	reduce 272',
    '	comma	reduce 272',
    '	semicolon	reduce 272',
    '	underscore	reduce 272',
    '	_ALTER	reduce 272',
    '	_AND	reduce 272',
    '	_COMMIT	reduce 272',
    '	_CONNECT	reduce 272',
    '	_CREATE	reduce 272',
    '	_CROSS	reduce 272',
    '	_DECLARE	reduce 272',
    '	_DELETE	reduce 272',
    '	_DISCONNECT	reduce 272',
    '	_DROP	reduce 272',
    '	_EXCEPT	reduce 272',
    '	_FOR	reduce 272',
    '	_FULL	reduce 272',
    '	_GRANT	reduce 272',
    '	_GROUP	reduce 272',
    '	_HAVING	reduce 272',
    '	_INNER	reduce 272',
    '	_INSERT	reduce 272',
    '	_INTERSECT	reduce 272',
    '	_JOIN	reduce 272',
    '	_LEFT	reduce 272',
    '	_NATURAL	reduce 272',
    '	_OR	reduce 272',
    '	_ORDER	reduce 272',
    '	_REVOKE	reduce 272',
    '	_RIGHT	reduce 272',
    '	_ROLLBACK	reduce 272',
    '	_SELECT	reduce 272',
    '	_SET	reduce 272',
    '	_TABLE	reduce 272',
    '	_THEN	reduce 272',
    '	_UNION	reduce 272',
    '	_UPDATE	reduce 272',
    '	_VALUES	reduce 272',
    '	_WHERE	reduce 272',
    '	_WITH	reduce 272',
    '	.	error',
    '',
    'state 632:',
    '',
    '	boolean_factor : boolean_test _	(270)',
    '',
    '	.	reduce 270',
    '',
    'state 633:',
    '',
    '	boolean_term : boolean_factor _	(268)',
    '',
    '	.	reduce 268',
    '',
    'state 634:',
    '',
    '	search_condition : boolean_term _	(266)',
    '	boolean_term : boolean_term _ _AND boolean_factor',
    '',
    '	_AND	shift 856',
    '	$end	reduce 266',
    '	identifier_body	reduce 266',
    '	delimited_identifier	reduce 266',
    '	left_paren	reduce 266',
    '	right_paren	reduce 266',
    '	comma	reduce 266',
    '	semicolon	reduce 266',
    '	underscore	reduce 266',
    '	_ALTER	reduce 266',
    '	_COMMIT	reduce 266',
    '	_CONNECT	reduce 266',
    '	_CREATE	reduce 266',
    '	_CROSS	reduce 266',
    '	_DECLARE	reduce 266',
    '	_DELETE	reduce 266',
    '	_DISCONNECT	reduce 266',
    '	_DROP	reduce 266',
    '	_EXCEPT	reduce 266',
    '	_FOR	reduce 266',
    '	_FULL	reduce 266',
    '	_GRANT	reduce 266',
    '	_GROUP	reduce 266',
    '	_HAVING	reduce 266',
    '	_INNER	reduce 266',
    '	_INSERT	reduce 266',
    '	_INTERSECT	reduce 266',
    '	_JOIN	reduce 266',
    '	_LEFT	reduce 266',
    '	_NATURAL	reduce 266',
    '	_OR	reduce 266',
    '	_ORDER	reduce 266',
    '	_REVOKE	reduce 266',
    '	_RIGHT	reduce 266',
    '	_ROLLBACK	reduce 266',
    '	_SELECT	reduce 266',
    '	_SET	reduce 266',
    '	_TABLE	reduce 266',
    '	_THEN	reduce 266',
    '	_UNION	reduce 266',
    '	_UPDATE	reduce 266',
    '	_VALUES	reduce 266',
    '	_WHERE	reduce 266',
    '	_WITH	reduce 266',
    '	.	error',
    '',
    'state 635:',
    '',
    '	searched_when_clause : _WHEN search_condition _ _THEN result',
    '	search_condition : search_condition _ _OR boolean_term',
    '',
    '	_OR	shift 857',
    '	_THEN	shift 858',
    '	.	error',
    '',
    'state 636:',
    '',
    '	boolean_primary : left_paren _ search_condition right_paren',
    '	row_value_constructor : left_paren _ row_value_constructor_list right_paren',
    '	primary_expression : left_paren _ expression right_paren',
    '	scalar_subquery : left_paren _ subquery right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 861',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXISTS	shift 637',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NOT	shift 638',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SELECT	shift 83',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TABLE	shift 85',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UNIQUE	shift 639',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_VALUES	shift 87',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	row_value_constructor_1	goto 617',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	explicit_table	goto 49',
    '	table_value_constructor	goto 50',
    '	query_specification	goto 51',
    '	table_subquery	goto 52',
    '	simple_table	goto 53',
    '	non_join_query_primary	goto 54',
    '	query_term	goto 55',
    '	non_join_query_term	goto 56',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	query_expression	goto 425',
    '	subquery	goto 426',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	row_value_constructor_list	goto 427',
    '	expression	goto 859',
    '	row_value_constructor	goto 619',
    '	overlaps_predicate	goto 620',
    '	match_predicate	goto 621',
    '	unique_predicate	goto 622',
    '	exists_predicate	goto 623',
    '	quantified_comparison_predicate	goto 624',
    '	null_predicate	goto 625',
    '	like_predicate	goto 626',
    '	in_predicate	goto 627',
    '	between_predicate	goto 628',
    '	comparison_predicate	goto 629',
    '	predicate	goto 630',
    '	boolean_primary	goto 631',
    '	boolean_test	goto 632',
    '	boolean_factor	goto 633',
    '	boolean_term	goto 634',
    '	search_condition	goto 860',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 637:',
    '',
    '	exists_predicate : _EXISTS _ table_subquery',
    '',
    '	left_paren	shift 68',
    '	.	error',
    '',
    '	table_subquery	goto 862',
    '',
    'state 638:',
    '',
    '	boolean_factor : _NOT _ boolean_test',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 636',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXISTS	shift 637',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UNIQUE	shift 639',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	row_value_constructor_1	goto 617',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 618',
    '	row_value_constructor	goto 619',
    '	overlaps_predicate	goto 620',
    '	match_predicate	goto 621',
    '	unique_predicate	goto 622',
    '	exists_predicate	goto 623',
    '	quantified_comparison_predicate	goto 624',
    '	null_predicate	goto 625',
    '	like_predicate	goto 626',
    '	in_predicate	goto 627',
    '	between_predicate	goto 628',
    '	comparison_predicate	goto 629',
    '	predicate	goto 630',
    '	boolean_primary	goto 631',
    '	boolean_test	goto 863',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 639:',
    '',
    '	unique_predicate : _UNIQUE _ table_subquery',
    '',
    '	left_paren	shift 68',
    '	.	error',
    '',
    '	table_subquery	goto 864',
    '',
    'state 640:',
    '',
    '	cast_specification : _CAST left_paren cast_operand _ _AS cast_target right_paren',
    '',
    '	_AS	shift 865',
    '	.	error',
    '',
    'state 641:',
    '',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	cast_operand : expression _	(460)',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	_AS	reduce 460',
    '	.	error',
    '',
    'state 642:',
    '',
    '	case_abbreviation : _COALESCE left_paren expression_list _ right_paren',
    '	expression_list : expression_list _ comma expression',
    '',
    '	right_paren	shift 866',
    '	comma	shift 867',
    '	.	error',
    '',
    'state 643:',
    '',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	expression_list : expression _	(445)',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	right_paren	reduce 445',
    '	comma	reduce 445',
    '	.	error',
    '',
    'state 644:',
    '',
    '	form_of_use_conversion : _CONVERT left_paren expression _ _USING form_of_use_conversion_name right_paren',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	_USING	shift 868',
    '	.	error',
    '',
    'state 645:',
    '',
    '	time_precision : time_fractional_seconds_precision _	(163)',
    '',
    '	.	reduce 163',
    '',
    'state 646:',
    '',
    '	current_time_value_function : _CURRENT_TIME left_paren time_precision _ right_paren',
    '',
    '	right_paren	shift 869',
    '	.	error',
    '',
    'state 647:',
    '',
    '	unsigned_integer : unsigned_integer _ digit',
    '	time_fractional_seconds_precision : unsigned_integer _	(164)',
    '',
    '	digit	shift 331',
    '	right_paren	reduce 164',
    '	.	error',
    '',
    'state 648:',
    '',
    '	timestamp_precision : time_fractional_seconds_precision _	(165)',
    '',
    '	.	reduce 165',
    '',
    'state 649:',
    '',
    '	current_timestamp_value_function : _CURRENT_TIMESTAMP left_paren timestamp_precision _ right_paren',
    '',
    '	right_paren	shift 870',
    '	.	error',
    '',
    'state 650:',
    '',
    '	extract_field : time_zone_field _	(495)',
    '',
    '	.	reduce 495',
    '',
    'state 651:',
    '',
    '	extract_field : datetime_field _	(494)',
    '',
    '	.	reduce 494',
    '',
    'state 652:',
    '',
    '	extract_expression : _EXTRACT left_paren extract_field _ _FROM extract_source right_paren',
    '',
    '	_FROM	shift 871',
    '	.	error',
    '',
    'state 653:',
    '',
    '	datetime_field : non_second_datetime_field _	(496)',
    '',
    '	.	reduce 496',
    '',
    'state 654:',
    '',
    '	datetime_field : _SECOND _	(497)',
    '',
    '	.	reduce 497',
    '',
    'state 655:',
    '',
    '	time_zone_field : _TIMEZONE_HOUR _	(498)',
    '',
    '	.	reduce 498',
    '',
    'state 656:',
    '',
    '	time_zone_field : _TIMEZONE_MINUTE _	(499)',
    '',
    '	.	reduce 499',
    '',
    'state 657:',
    '',
    '	fold : _LOWER left_paren expression _ right_paren',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '',
    '	concatenation_operator	shift 421',
    '	right_paren	shift 872',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	.	error',
    '',
    'state 658:',
    '',
    '	case_abbreviation : _NULLIF left_paren expression _ comma expression right_paren',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	comma	shift 873',
    '	minus_sign	shift 423',
    '	.	error',
    '',
    'state 659:',
    '',
    '	octet_length_expression : _OCTET_LENGTH left_paren expression _ right_paren',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '',
    '	concatenation_operator	shift 421',
    '	right_paren	shift 874',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	.	error',
    '',
    'state 660:',
    '',
    '	position_expression : _POSITION left_paren expression _ _IN expression right_paren',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	_IN	shift 875',
    '	.	error',
    '',
    'state 661:',
    '',
    '	character_bit_substring_function : _SUBSTRING left_paren expression _ _FROM start_position for_strlength_opt right_paren',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	_FROM	shift 876',
    '	.	error',
    '',
    'state 662:',
    '',
    '	character_translation : _TRANSLATE left_paren expression _ _USING translation_name right_paren',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	_USING	shift 877',
    '	.	error',
    '',
    'state 663:',
    '',
    '	trim_operands : trim_character _ _FROM trim_source',
    '',
    '	_FROM	shift 878',
    '	.	error',
    '',
    'state 664:',
    '',
    '	trim_operands : trim_specification _ _FROM trim_source',
    '	trim_operands : trim_specification _ trim_character _FROM trim_source',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_FROM	shift 881',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_character	goto 879',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 880',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 665:',
    '',
    '	trim_operands : trim_source _	(484)',
    '',
    '	.	reduce 484',
    '',
    'state 666:',
    '',
    '	trim_function : _TRIM left_paren trim_operands _ right_paren',
    '',
    '	right_paren	shift 882',
    '	.	error',
    '',
    'state 667:',
    '',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	trim_character : expression _	(491)',
    '	trim_source : expression _	(492)',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	_FROM	reduce 491',
    '	right_paren	reduce 492',
    '	.	error',
    '',
    'state 668:',
    '',
    '	trim_specification : _BOTH _	(490)',
    '',
    '	.	reduce 490',
    '',
    'state 669:',
    '',
    '	trim_specification : _LEADING _	(488)',
    '',
    '	.	reduce 488',
    '',
    'state 670:',
    '',
    '	trim_specification : _TRAILING _	(489)',
    '',
    '	.	reduce 489',
    '',
    'state 671:',
    '',
    '	fold : _UPPER left_paren expression _ right_paren',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '',
    '	concatenation_operator	shift 421',
    '	right_paren	shift 883',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	.	error',
    '',
    'state 672:',
    '',
    '	non_join_query_term : query_term _INTERSECT all_opt corresponding_spec_opt query_primary _	(355)',
    '',
    '	.	reduce 355',
    '',
    'state 673:',
    '',
    '	query_primary : non_join_query_primary _	(440)',
    '',
    '	.	reduce 440',
    '',
    'state 674:',
    '',
    '	corresponding_spec : _CORRESPONDING corresponding_column_list_opt _	(436)',
    '',
    '	.	reduce 436',
    '',
    'state 675:',
    '',
    '	corresponding_column_list_opt : _BY _ left_paren corresponding_column_list right_paren',
    '',
    '	left_paren	shift 884',
    '	.	error',
    '',
    'state 676:',
    '',
    '	query_expression : query_expression _EXCEPT all_opt corresponding_spec_opt query_term _	(353)',
    '	non_join_query_term : query_term _ _INTERSECT all_opt corresponding_spec_opt query_primary',
    '',
    '	_INTERSECT	shift 89',
    '	$end	reduce 353',
    '	identifier_body	reduce 353',
    '	delimited_identifier	reduce 353',
    '	left_paren	reduce 353',
    '	right_paren	reduce 353',
    '	semicolon	reduce 353',
    '	underscore	reduce 353',
    '	_ALTER	reduce 353',
    '	_COMMIT	reduce 353',
    '	_CONNECT	reduce 353',
    '	_CREATE	reduce 353',
    '	_DECLARE	reduce 353',
    '	_DELETE	reduce 353',
    '	_DISCONNECT	reduce 353',
    '	_DROP	reduce 353',
    '	_EXCEPT	reduce 353',
    '	_FOR	reduce 353',
    '	_GRANT	reduce 353',
    '	_INSERT	reduce 353',
    '	_ORDER	reduce 353',
    '	_REVOKE	reduce 353',
    '	_ROLLBACK	reduce 353',
    '	_SELECT	reduce 353',
    '	_SET	reduce 353',
    '	_TABLE	reduce 353',
    '	_UNION	reduce 353',
    '	_UPDATE	reduce 353',
    '	_VALUES	reduce 353',
    '	_WITH	reduce 353',
    '	.	error',
    '',
    'state 677:',
    '',
    '	query_term : non_join_query_term _	(435)',
    '',
    '	.	reduce 435',
    '',
    'state 678:',
    '',
    '	collate_clause_opt : collate_clause _	(99)',
    '',
    '	.	reduce 99',
    '',
    'state 679:',
    '',
    '	sort_specification : sort_key collate_clause_opt _ ordering_specification_opt',
    '	ordering_specification_opt : _	(594)',
    '',
    '	_ASC	shift 886',
    '	_DESC	shift 887',
    '	$end	reduce 594',
    '	identifier_body	reduce 594',
    '	delimited_identifier	reduce 594',
    '	left_paren	reduce 594',
    '	comma	reduce 594',
    '	underscore	reduce 594',
    '	_ALTER	reduce 594',
    '	_COMMIT	reduce 594',
    '	_CONNECT	reduce 594',
    '	_CREATE	reduce 594',
    '	_DECLARE	reduce 594',
    '	_DELETE	reduce 594',
    '	_DISCONNECT	reduce 594',
    '	_DROP	reduce 594',
    '	_FOR	reduce 594',
    '	_GRANT	reduce 594',
    '	_INSERT	reduce 594',
    '	_REVOKE	reduce 594',
    '	_ROLLBACK	reduce 594',
    '	_SELECT	reduce 594',
    '	_SET	reduce 594',
    '	_TABLE	reduce 594',
    '	_UPDATE	reduce 594',
    '	_VALUES	reduce 594',
    '	.	error',
    '',
    '	ordering_specification_opt	goto 885',
    '',
    'state 680:',
    '',
    '	sort_specification_list : sort_specification_list comma _ sort_specification',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	sort_key	goto 455',
    '	sort_specification	goto 888',
    '	column_name	goto 458',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	unsigned_integer	goto 460',
    '	regular_identifier	goto 64',
    '',
    'state 681:',
    '',
    '	query_expression : query_expression _UNION all_opt corresponding_spec_opt query_term _	(352)',
    '	non_join_query_term : query_term _ _INTERSECT all_opt corresponding_spec_opt query_primary',
    '',
    '	_INTERSECT	shift 89',
    '	$end	reduce 352',
    '	identifier_body	reduce 352',
    '	delimited_identifier	reduce 352',
    '	left_paren	reduce 352',
    '	right_paren	reduce 352',
    '	semicolon	reduce 352',
    '	underscore	reduce 352',
    '	_ALTER	reduce 352',
    '	_COMMIT	reduce 352',
    '	_CONNECT	reduce 352',
    '	_CREATE	reduce 352',
    '	_DECLARE	reduce 352',
    '	_DELETE	reduce 352',
    '	_DISCONNECT	reduce 352',
    '	_DROP	reduce 352',
    '	_EXCEPT	reduce 352',
    '	_FOR	reduce 352',
    '	_GRANT	reduce 352',
    '	_INSERT	reduce 352',
    '	_ORDER	reduce 352',
    '	_REVOKE	reduce 352',
    '	_ROLLBACK	reduce 352',
    '	_SELECT	reduce 352',
    '	_SET	reduce 352',
    '	_TABLE	reduce 352',
    '	_UNION	reduce 352',
    '	_UPDATE	reduce 352',
    '	_VALUES	reduce 352',
    '	_WITH	reduce 352',
    '	.	error',
    '',
    'state 682:',
    '',
    '	module_opt : temporary_table_declaration module_contents _	(62)',
    '',
    '	.	reduce 62',
    '',
    'state 683:',
    '',
    '	declare_cursor : _DECLARE _ cursor_name insensitive_opt scroll_opt _CURSOR _FOR cursor_specification',
    '	dynamic_declare_cursor : _DECLARE _ cursor_name insensitive_opt scroll_opt _CURSOR _FOR statement_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	cursor_name	goto 684',
    '	actual_identifier	goto 61',
    '	identifier	goto 685',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 684:',
    '',
    '	declare_cursor : _DECLARE cursor_name _ insensitive_opt scroll_opt _CURSOR _FOR cursor_specification',
    '	dynamic_declare_cursor : _DECLARE cursor_name _ insensitive_opt scroll_opt _CURSOR _FOR statement_name',
    '	insensitive_opt : _	(581)',
    '',
    '	_INSENSITIVE	shift 890',
    '	_CURSOR	reduce 581',
    '	_SCROLL	reduce 581',
    '	.	error',
    '',
    '	insensitive_opt	goto 889',
    '',
    'state 685:',
    '',
    '	cursor_name : identifier _	(585)',
    '',
    '	.	reduce 585',
    '',
    'state 686:',
    '',
    '	procedure : _PROCEDURE procedure_name _ parameter_declaration_list semicolon SQL_procedure_statement semicolon',
    '',
    '	left_paren	shift 892',
    '	.	error',
    '',
    '	parameter_declaration_list	goto 891',
    '',
    'state 687:',
    '',
    '	procedure_name : identifier _	(605)',
    '',
    '	.	reduce 605',
    '',
    'state 688:',
    '',
    '	module_authorization_clause : _SCHEMA schema_name _AUTHORIZATION _ module_authorization_identifier',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	authorization_identifier	goto 470',
    '	module_authorization_identifier	goto 893',
    '	actual_identifier	goto 61',
    '	identifier	goto 472',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 689:',
    '',
    '	character_set_name : identifier period identifier period _ SQL_language_identifier',
    '',
    '	identifier_body	shift 66',
    '	.	error',
    '',
    '	SQL_language_identifier	goto 894',
    '	regular_identifier	goto 895',
    '',
    'state 690:',
    '',
    '	add_domain_constraint_definition : _ADD domain_constraint _	(771)',
    '',
    '	.	reduce 771',
    '',
    'state 691:',
    '',
    '	constraint_name_definition_opt : constraint_name_definition _	(230)',
    '',
    '	.	reduce 230',
    '',
    'state 692:',
    '',
    '	domain_constraint : constraint_name_definition_opt _ check_constraint_definition constraint_attributes_opt',
    '',
    '	_CHECK	shift 897',
    '	.	error',
    '',
    '	check_constraint_definition	goto 896',
    '',
    'state 693:',
    '',
    '	constraint_name_definition : _CONSTRAINT _ constraint_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	constraint_name	goto 898',
    '	qualified_name	goto 310',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 694:',
    '',
    '	drop_domain_constraint_definition : _DROP _CONSTRAINT _ constraint_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	constraint_name	goto 899',
    '	qualified_name	goto 310',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 695:',
    '',
    '	drop_domain_default_clause : _DROP _DEFAULT _	(770)',
    '',
    '	.	reduce 770',
    '',
    'state 696:',
    '',
    '	set_domain_default_clause : _SET default_clause _	(769)',
    '',
    '	.	reduce 769',
    '',
    'state 697:',
    '',
    '	default_clause : _DEFAULT _ default_option',
    '',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	digit	shift 147',
    '	plus_sign	shift 148',
    '	minus_sign	shift 149',
    '	period	shift 150',
    '	underscore	shift 69',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 903',
    '	_DATE	shift 154',
    '	_INTERVAL	shift 156',
    '	_NULL	shift 904',
    '	_SESSION_USER	shift 905',
    '	_SYSTEM_USER	shift 906',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_USER	shift 907',
    '	.	error',
    '',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 128',
    '	signed_numeric_literal	goto 129',
    '	datetime_value_function	goto 900',
    '	literal	goto 901',
    '	default_option	goto 902',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 132',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	sign	goto 137',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 142',
    '',
    'state 698:',
    '',
    '	add_column_definition : _ADD column_opt _ column_definition',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	column_name	goto 908',
    '	column_definition	goto 909',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 699:',
    '',
    '	table_constraint_definition : constraint_name_definition_opt _ table_constraint constraint_check_time_opt',
    '',
    '	_CHECK	shift 897',
    '	_FOREIGN	shift 915',
    '	_PRIMARY	shift 916',
    '	_UNIQUE	shift 917',
    '	.	error',
    '',
    '	referential_constraint_definition	goto 910',
    '	unique_constraint_definition	goto 911',
    '	table_constraint	goto 912',
    '	check_constraint_definition	goto 913',
    '	unique_specification	goto 914',
    '',
    'state 700:',
    '',
    '	add_table_constraint_definition : _ADD table_constraint_definition _	(757)',
    '',
    '	.	reduce 757',
    '',
    'state 701:',
    '',
    '	column_opt : _COLUMN _	(749)',
    '',
    '	.	reduce 749',
    '',
    'state 702:',
    '',
    '	alter_column_definition : _ALTER column_opt _ column_name alter_column_action',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	column_name	goto 918',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 703:',
    '',
    '	drop_column_definition : _DROP column_opt _ column_name drop_behaviour',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	column_name	goto 919',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 704:',
    '',
    '	drop_table_constraint_definition : _DROP _CONSTRAINT _ constraint_name drop_behaviour',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	constraint_name	goto 920',
    '	qualified_name	goto 310',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 705:',
    '',
    '	connection_target : SQL_server_name connection_name_opt user_name_opt _	(856)',
    '',
    '	.	reduce 856',
    '',
    'state 706:',
    '',
    '	user_name_opt : _USER _ user_name',
    '',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	digit	shift 147',
    '	plus_sign	shift 148',
    '	minus_sign	shift 149',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_DATE	shift 154',
    '	_INTERVAL	shift 156',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	.	error',
    '',
    '	user_name	goto 921',
    '	simple_value_specification	goto 922',
    '	parameter_name	goto 122',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 128',
    '	signed_numeric_literal	goto 129',
    '	literal	goto 130',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 132',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	sign	goto 137',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 142',
    '',
    'state 707:',
    '',
    '	connection_name_opt : _AS connection_name _	(859)',
    '',
    '	.	reduce 859',
    '',
    'state 708:',
    '',
    '	table_definition : _CREATE table_definition_opts _TABLE table_name table_element_list _ table_commit_opts',
    '	table_commit_opts : _	(655)',
    '',
    '	_ON	shift 924',
    '	$end	reduce 655',
    '	identifier_body	reduce 655',
    '	delimited_identifier	reduce 655',
    '	left_paren	reduce 655',
    '	semicolon	reduce 655',
    '	underscore	reduce 655',
    '	_ALTER	reduce 655',
    '	_COMMIT	reduce 655',
    '	_CONNECT	reduce 655',
    '	_CREATE	reduce 655',
    '	_DECLARE	reduce 655',
    '	_DELETE	reduce 655',
    '	_DISCONNECT	reduce 655',
    '	_DROP	reduce 655',
    '	_GRANT	reduce 655',
    '	_INSERT	reduce 655',
    '	_REVOKE	reduce 655',
    '	_ROLLBACK	reduce 655',
    '	_SELECT	reduce 655',
    '	_SET	reduce 655',
    '	_TABLE	reduce 655',
    '	_UPDATE	reduce 655',
    '	_VALUES	reduce 655',
    '	.	error',
    '',
    '	table_commit_opts	goto 923',
    '',
    'state 709:',
    '',
    '	table_element_list : left_paren _ table_element table_element_list_opt right_paren',
    '	constraint_name_definition_opt : _	(229)',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_CONSTRAINT	shift 693',
    '	_CHECK	reduce 229',
    '	_FOREIGN	reduce 229',
    '	_PRIMARY	reduce 229',
    '	_UNIQUE	reduce 229',
    '	.	error',
    '',
    '	constraint_name_definition	goto 691',
    '	constraint_name_definition_opt	goto 699',
    '	column_name	goto 908',
    '	table_constraint_definition	goto 925',
    '	column_definition	goto 926',
    '	table_element	goto 927',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 710:',
    '',
    '	constraint_attributes : constraint_check_time _ deferrable_opt',
    '	deferrable_opt : _	(562)',
    '',
    '	_DEFERRABLE	shift 929',
    '	_NOT	shift 930',
    '	$end	reduce 562',
    '	identifier_body	reduce 562',
    '	delimited_identifier	reduce 562',
    '	left_paren	reduce 562',
    '	right_paren	reduce 562',
    '	comma	reduce 562',
    '	semicolon	reduce 562',
    '	underscore	reduce 562',
    '	_ALTER	reduce 562',
    '	_COLLATE	reduce 562',
    '	_COMMIT	reduce 562',
    '	_CONNECT	reduce 562',
    '	_CREATE	reduce 562',
    '	_DECLARE	reduce 562',
    '	_DELETE	reduce 562',
    '	_DISCONNECT	reduce 562',
    '	_DROP	reduce 562',
    '	_GRANT	reduce 562',
    '	_INSERT	reduce 562',
    '	_REVOKE	reduce 562',
    '	_ROLLBACK	reduce 562',
    '	_SELECT	reduce 562',
    '	_SET	reduce 562',
    '	_TABLE	reduce 562',
    '	_UPDATE	reduce 562',
    '	_VALUES	reduce 562',
    '	.	error',
    '',
    '	deferrable_opt	goto 928',
    '',
    'state 711:',
    '',
    '	constraint_attributes_opt : constraint_attributes _	(559)',
    '',
    '	.	reduce 559',
    '',
    'state 712:',
    '',
    '	assertion_definition : _CREATE _ASSERTION constraint_name assertion_check constraint_attributes_opt _	(693)',
    '',
    '	.	reduce 693',
    '',
    'state 713:',
    '',
    '	constraint_attributes : _DEFERRABLE _ constraint_check_time_opt',
    '	constraint_check_time_opt : _	(565)',
    '',
    '	_INITIALLY	shift 714',
    '	$end	reduce 565',
    '	identifier_body	reduce 565',
    '	delimited_identifier	reduce 565',
    '	left_paren	reduce 565',
    '	right_paren	reduce 565',
    '	comma	reduce 565',
    '	semicolon	reduce 565',
    '	underscore	reduce 565',
    '	_ALTER	reduce 565',
    '	_COLLATE	reduce 565',
    '	_COMMIT	reduce 565',
    '	_CONNECT	reduce 565',
    '	_CREATE	reduce 565',
    '	_DECLARE	reduce 565',
    '	_DELETE	reduce 565',
    '	_DISCONNECT	reduce 565',
    '	_DROP	reduce 565',
    '	_GRANT	reduce 565',
    '	_INSERT	reduce 565',
    '	_REVOKE	reduce 565',
    '	_ROLLBACK	reduce 565',
    '	_SELECT	reduce 565',
    '	_SET	reduce 565',
    '	_TABLE	reduce 565',
    '	_UPDATE	reduce 565',
    '	_VALUES	reduce 565',
    '	.	error',
    '',
    '	constraint_check_time_opt	goto 931',
    '	constraint_check_time	goto 932',
    '',
    'state 714:',
    '',
    '	constraint_check_time : _INITIALLY _ _DEFERRED',
    '	constraint_check_time : _INITIALLY _ _IMMEDIATE',
    '',
    '	_DEFERRED	shift 933',
    '	_IMMEDIATE	shift 934',
    '	.	error',
    '',
    'state 715:',
    '',
    '	assertion_check : _CHECK left_paren _ search_condition right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 636',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXISTS	shift 637',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NOT	shift 638',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UNIQUE	shift 639',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	row_value_constructor_1	goto 617',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 618',
    '	row_value_constructor	goto 619',
    '	overlaps_predicate	goto 620',
    '	match_predicate	goto 621',
    '	unique_predicate	goto 622',
    '	exists_predicate	goto 623',
    '	quantified_comparison_predicate	goto 624',
    '	null_predicate	goto 625',
    '	like_predicate	goto 626',
    '	in_predicate	goto 627',
    '	between_predicate	goto 628',
    '	comparison_predicate	goto 629',
    '	predicate	goto 630',
    '	boolean_primary	goto 631',
    '	boolean_test	goto 632',
    '	boolean_factor	goto 633',
    '	boolean_term	goto 634',
    '	search_condition	goto 935',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 716:',
    '',
    '	character_set_definition : _CREATE _CHARACTER _SET character_set_name as_opt _ character_set_source charset_collation_opt',
    '',
    '	_GET	shift 937',
    '	.	error',
    '',
    '	character_set_source	goto 936',
    '',
    'state 717:',
    '',
    '	collation_definition : _CREATE _COLLATION collation_name _FOR character_set_specification _ _FROM collation_source pad_attribute_opt',
    '',
    '	_FROM	shift 938',
    '	.	error',
    '',
    'state 718:',
    '',
    '	numeric_type : approximate_numeric_type _	(138)',
    '',
    '	.	reduce 138',
    '',
    'state 719:',
    '',
    '	numeric_type : exact_numeric_type _	(137)',
    '',
    '	.	reduce 137',
    '',
    'state 720:',
    '',
    '	data_type : interval_type _	(106)',
    '',
    '	.	reduce 106',
    '',
    'state 721:',
    '',
    '	data_type : datetime_type _	(105)',
    '',
    '	.	reduce 105',
    '',
    'state 722:',
    '',
    '	data_type : numeric_type _	(104)',
    '',
    '	.	reduce 104',
    '',
    'state 723:',
    '',
    '	data_type : bit_string_type _	(103)',
    '',
    '	.	reduce 103',
    '',
    'state 724:',
    '',
    '	data_type : national_character_string_type _	(102)',
    '',
    '	.	reduce 102',
    '',
    'state 725:',
    '',
    '	data_type : character_string_type _ data_type_opt',
    '	data_type_opt : _	(107)',
    '',
    '	_CHARACTER	shift 940',
    '	$end	reduce 107',
    '	identifier_body	reduce 107',
    '	delimited_identifier	reduce 107',
    '	left_paren	reduce 107',
    '	right_paren	reduce 107',
    '	comma	reduce 107',
    '	semicolon	reduce 107',
    '	underscore	reduce 107',
    '	_ALTER	reduce 107',
    '	_CHECK	reduce 107',
    '	_COLLATE	reduce 107',
    '	_COMMIT	reduce 107',
    '	_CONNECT	reduce 107',
    '	_CONSTRAINT	reduce 107',
    '	_CREATE	reduce 107',
    '	_DECLARE	reduce 107',
    '	_DEFAULT	reduce 107',
    '	_DELETE	reduce 107',
    '	_DISCONNECT	reduce 107',
    '	_DROP	reduce 107',
    '	_GRANT	reduce 107',
    '	_INSERT	reduce 107',
    '	_NOT	reduce 107',
    '	_PRIMARY	reduce 107',
    '	_REFERENCES	reduce 107',
    '	_REVOKE	reduce 107',
    '	_ROLLBACK	reduce 107',
    '	_SELECT	reduce 107',
    '	_SET	reduce 107',
    '	_TABLE	reduce 107',
    '	_UNIQUE	reduce 107',
    '	_UPDATE	reduce 107',
    '	_VALUES	reduce 107',
    '	.	error',
    '',
    '	data_type_opt	goto 939',
    '',
    'state 726:',
    '',
    '	domain_definition : _CREATE _DOMAIN domain_name as_opt data_type _ default_clause_opt domain_constraint_opt collate_clause_opt',
    '	default_clause_opt : _	(94)',
    '',
    '	_DEFAULT	shift 697',
    '	$end	reduce 94',
    '	identifier_body	reduce 94',
    '	delimited_identifier	reduce 94',
    '	left_paren	reduce 94',
    '	semicolon	reduce 94',
    '	underscore	reduce 94',
    '	_ALTER	reduce 94',
    '	_CHECK	reduce 94',
    '	_COLLATE	reduce 94',
    '	_COMMIT	reduce 94',
    '	_CONNECT	reduce 94',
    '	_CONSTRAINT	reduce 94',
    '	_CREATE	reduce 94',
    '	_DECLARE	reduce 94',
    '	_DELETE	reduce 94',
    '	_DISCONNECT	reduce 94',
    '	_DROP	reduce 94',
    '	_GRANT	reduce 94',
    '	_INSERT	reduce 94',
    '	_REVOKE	reduce 94',
    '	_ROLLBACK	reduce 94',
    '	_SELECT	reduce 94',
    '	_SET	reduce 94',
    '	_TABLE	reduce 94',
    '	_UPDATE	reduce 94',
    '	_VALUES	reduce 94',
    '	.	error',
    '',
    '	default_clause	goto 941',
    '	default_clause_opt	goto 942',
    '',
    'state 727:',
    '',
    '	*** conflicts:',
    '',
    '	shift 944, reduce 135 on left_paren',
    '',
    '	bit_string_type : _BIT _ character_string_type_len',
    '	bit_string_type : _BIT _ _VARYING character_string_type_len',
    '	bit_string_type : _BIT _	(135)',
    '	bit_string_type : _BIT _ _VARYING',
    '',
    '	left_paren	shift 944',
    '	_VARYING	shift 945',
    '	$end	reduce 135',
    '	identifier_body	reduce 135',
    '	delimited_identifier	reduce 135',
    '	right_paren	reduce 135',
    '	comma	reduce 135',
    '	semicolon	reduce 135',
    '	underscore	reduce 135',
    '	_ALTER	reduce 135',
    '	_CHECK	reduce 135',
    '	_COLLATE	reduce 135',
    '	_COMMIT	reduce 135',
    '	_CONNECT	reduce 135',
    '	_CONSTRAINT	reduce 135',
    '	_CREATE	reduce 135',
    '	_DECLARE	reduce 135',
    '	_DEFAULT	reduce 135',
    '	_DELETE	reduce 135',
    '	_DISCONNECT	reduce 135',
    '	_DROP	reduce 135',
    '	_GRANT	reduce 135',
    '	_INSERT	reduce 135',
    '	_NOT	reduce 135',
    '	_PRIMARY	reduce 135',
    '	_REFERENCES	reduce 135',
    '	_REVOKE	reduce 135',
    '	_ROLLBACK	reduce 135',
    '	_SELECT	reduce 135',
    '	_SET	reduce 135',
    '	_TABLE	reduce 135',
    '	_UNIQUE	reduce 135',
    '	_UPDATE	reduce 135',
    '	_VALUES	reduce 135',
    '	.	error',
    '',
    '	character_string_type_len	goto 943',
    '',
    'state 728:',
    '',
    '	*** conflicts:',
    '',
    '	shift 944, reduce 115 on left_paren',
    '',
    '	character_string_type : _CHAR _ character_string_type_len',
    '	character_string_type : _CHAR _ _VARYING character_string_type_len',
    '	character_string_type : _CHAR _	(115)',
    '	character_string_type : _CHAR _ _VARYING',
    '',
    '	left_paren	shift 944',
    '	_VARYING	shift 947',
    '	$end	reduce 115',
    '	identifier_body	reduce 115',
    '	delimited_identifier	reduce 115',
    '	right_paren	reduce 115',
    '	comma	reduce 115',
    '	semicolon	reduce 115',
    '	underscore	reduce 115',
    '	_ALTER	reduce 115',
    '	_CHARACTER	reduce 115',
    '	_CHECK	reduce 115',
    '	_COLLATE	reduce 115',
    '	_COMMIT	reduce 115',
    '	_CONNECT	reduce 115',
    '	_CONSTRAINT	reduce 115',
    '	_CREATE	reduce 115',
    '	_DECLARE	reduce 115',
    '	_DEFAULT	reduce 115',
    '	_DELETE	reduce 115',
    '	_DISCONNECT	reduce 115',
    '	_DROP	reduce 115',
    '	_GRANT	reduce 115',
    '	_INSERT	reduce 115',
    '	_NOT	reduce 115',
    '	_PRIMARY	reduce 115',
    '	_REFERENCES	reduce 115',
    '	_REVOKE	reduce 115',
    '	_ROLLBACK	reduce 115',
    '	_SELECT	reduce 115',
    '	_SET	reduce 115',
    '	_TABLE	reduce 115',
    '	_UNIQUE	reduce 115',
    '	_UPDATE	reduce 115',
    '	_VALUES	reduce 115',
    '	.	error',
    '',
    '	character_string_type_len	goto 946',
    '',
    'state 729:',
    '',
    '	*** conflicts:',
    '',
    '	shift 944, reduce 114 on left_paren',
    '',
    '	character_string_type : _CHARACTER _ character_string_type_len',
    '	character_string_type : _CHARACTER _ _VARYING character_string_type_len',
    '	character_string_type : _CHARACTER _	(114)',
    '	character_string_type : _CHARACTER _ _VARYING',
    '',
    '	left_paren	shift 944',
    '	_VARYING	shift 949',
    '	$end	reduce 114',
    '	identifier_body	reduce 114',
    '	delimited_identifier	reduce 114',
    '	right_paren	reduce 114',
    '	comma	reduce 114',
    '	semicolon	reduce 114',
    '	underscore	reduce 114',
    '	_ALTER	reduce 114',
    '	_CHARACTER	reduce 114',
    '	_CHECK	reduce 114',
    '	_COLLATE	reduce 114',
    '	_COMMIT	reduce 114',
    '	_CONNECT	reduce 114',
    '	_CONSTRAINT	reduce 114',
    '	_CREATE	reduce 114',
    '	_DECLARE	reduce 114',
    '	_DEFAULT	reduce 114',
    '	_DELETE	reduce 114',
    '	_DISCONNECT	reduce 114',
    '	_DROP	reduce 114',
    '	_GRANT	reduce 114',
    '	_INSERT	reduce 114',
    '	_NOT	reduce 114',
    '	_PRIMARY	reduce 114',
    '	_REFERENCES	reduce 114',
    '	_REVOKE	reduce 114',
    '	_ROLLBACK	reduce 114',
    '	_SELECT	reduce 114',
    '	_SET	reduce 114',
    '	_TABLE	reduce 114',
    '	_UNIQUE	reduce 114',
    '	_UPDATE	reduce 114',
    '	_VALUES	reduce 114',
    '	.	error',
    '',
    '	character_string_type_len	goto 948',
    '',
    'state 730:',
    '',
    '	datetime_type : _DATE _	(154)',
    '',
    '	.	reduce 154',
    '',
    'state 731:',
    '',
    '	*** conflicts:',
    '',
    '	shift 951, reduce 145 on left_paren',
    '',
    '	exact_numeric_type : _DEC _ numeric_precision_scale_opt',
    '	numeric_precision_scale_opt : _	(145)',
    '',
    '	left_paren	shift 951',
    '	$end	reduce 145',
    '	identifier_body	reduce 145',
    '	delimited_identifier	reduce 145',
    '	right_paren	reduce 145',
    '	comma	reduce 145',
    '	semicolon	reduce 145',
    '	underscore	reduce 145',
    '	_ALTER	reduce 145',
    '	_CHECK	reduce 145',
    '	_COLLATE	reduce 145',
    '	_COMMIT	reduce 145',
    '	_CONNECT	reduce 145',
    '	_CONSTRAINT	reduce 145',
    '	_CREATE	reduce 145',
    '	_DECLARE	reduce 145',
    '	_DEFAULT	reduce 145',
    '	_DELETE	reduce 145',
    '	_DISCONNECT	reduce 145',
    '	_DROP	reduce 145',
    '	_GRANT	reduce 145',
    '	_INSERT	reduce 145',
    '	_NOT	reduce 145',
    '	_PRIMARY	reduce 145',
    '	_REFERENCES	reduce 145',
    '	_REVOKE	reduce 145',
    '	_ROLLBACK	reduce 145',
    '	_SELECT	reduce 145',
    '	_SET	reduce 145',
    '	_TABLE	reduce 145',
    '	_UNIQUE	reduce 145',
    '	_UPDATE	reduce 145',
    '	_VALUES	reduce 145',
    '	.	error',
    '',
    '	numeric_precision_scale_opt	goto 950',
    '',
    'state 732:',
    '',
    '	*** conflicts:',
    '',
    '	shift 951, reduce 145 on left_paren',
    '',
    '	exact_numeric_type : _DECIMAL _ numeric_precision_scale_opt',
    '	numeric_precision_scale_opt : _	(145)',
    '',
    '	left_paren	shift 951',
    '	$end	reduce 145',
    '	identifier_body	reduce 145',
    '	delimited_identifier	reduce 145',
    '	right_paren	reduce 145',
    '	comma	reduce 145',
    '	semicolon	reduce 145',
    '	underscore	reduce 145',
    '	_ALTER	reduce 145',
    '	_CHECK	reduce 145',
    '	_COLLATE	reduce 145',
    '	_COMMIT	reduce 145',
    '	_CONNECT	reduce 145',
    '	_CONSTRAINT	reduce 145',
    '	_CREATE	reduce 145',
    '	_DECLARE	reduce 145',
    '	_DEFAULT	reduce 145',
    '	_DELETE	reduce 145',
    '	_DISCONNECT	reduce 145',
    '	_DROP	reduce 145',
    '	_GRANT	reduce 145',
    '	_INSERT	reduce 145',
    '	_NOT	reduce 145',
    '	_PRIMARY	reduce 145',
    '	_REFERENCES	reduce 145',
    '	_REVOKE	reduce 145',
    '	_ROLLBACK	reduce 145',
    '	_SELECT	reduce 145',
    '	_SET	reduce 145',
    '	_TABLE	reduce 145',
    '	_UNIQUE	reduce 145',
    '	_UPDATE	reduce 145',
    '	_VALUES	reduce 145',
    '	.	error',
    '',
    '	numeric_precision_scale_opt	goto 952',
    '',
    'state 733:',
    '',
    '	approximate_numeric_type : _DOUBLE _ _PRECISION',
    '',
    '	_PRECISION	shift 953',
    '	.	error',
    '',
    'state 734:',
    '',
    '	*** conflicts:',
    '',
    '	shift 954, reduce 150 on left_paren',
    '',
    '	approximate_numeric_type : _FLOAT _	(150)',
    '	approximate_numeric_type : _FLOAT _ left_paren precision right_paren',
    '',
    '	left_paren	shift 954',
    '	$end	reduce 150',
    '	identifier_body	reduce 150',
    '	delimited_identifier	reduce 150',
    '	right_paren	reduce 150',
    '	comma	reduce 150',
    '	semicolon	reduce 150',
    '	underscore	reduce 150',
    '	_ALTER	reduce 150',
    '	_CHECK	reduce 150',
    '	_COLLATE	reduce 150',
    '	_COMMIT	reduce 150',
    '	_CONNECT	reduce 150',
    '	_CONSTRAINT	reduce 150',
    '	_CREATE	reduce 150',
    '	_DECLARE	reduce 150',
    '	_DEFAULT	reduce 150',
    '	_DELETE	reduce 150',
    '	_DISCONNECT	reduce 150',
    '	_DROP	reduce 150',
    '	_GRANT	reduce 150',
    '	_INSERT	reduce 150',
    '	_NOT	reduce 150',
    '	_PRIMARY	reduce 150',
    '	_REFERENCES	reduce 150',
    '	_REVOKE	reduce 150',
    '	_ROLLBACK	reduce 150',
    '	_SELECT	reduce 150',
    '	_SET	reduce 150',
    '	_TABLE	reduce 150',
    '	_UNIQUE	reduce 150',
    '	_UPDATE	reduce 150',
    '	_VALUES	reduce 150',
    '	.	error',
    '',
    'state 735:',
    '',
    '	exact_numeric_type : _INT _	(143)',
    '',
    '	.	reduce 143',
    '',
    'state 736:',
    '',
    '	exact_numeric_type : _INTEGER _	(142)',
    '',
    '	.	reduce 142',
    '',
    'state 737:',
    '',
    '	interval_type : _INTERVAL _ interval_qualifier',
    '',
    '	_DAY	shift 415',
    '	_HOUR	shift 416',
    '	_MINUTE	shift 417',
    '	_MONTH	shift 418',
    '	_SECOND	shift 419',
    '	_YEAR	shift 420',
    '	.	error',
    '',
    '	non_second_datetime_field	goto 409',
    '	start_field	goto 410',
    '	interval_qualifier	goto 955',
    '',
    'state 738:',
    '',
    '	national_character_string_type : _NATIONAL _ _CHARACTER character_string_type_len',
    '	national_character_string_type : _NATIONAL _ _CHAR character_string_type_len',
    '	national_character_string_type : _NATIONAL _ _CHARACTER _VARYING character_string_type_len',
    '	national_character_string_type : _NATIONAL _ _CHAR _VARYING character_string_type_len',
    '	national_character_string_type : _NATIONAL _ _CHARACTER',
    '	national_character_string_type : _NATIONAL _ _CHAR',
    '	national_character_string_type : _NATIONAL _ _CHARACTER _VARYING',
    '	national_character_string_type : _NATIONAL _ _CHAR _VARYING',
    '',
    '	_CHAR	shift 956',
    '	_CHARACTER	shift 957',
    '	.	error',
    '',
    'state 739:',
    '',
    '	*** conflicts:',
    '',
    '	shift 944, reduce 129 on left_paren',
    '',
    '	national_character_string_type : _NCHAR _ character_string_type_len',
    '	national_character_string_type : _NCHAR _ _VARYING character_string_type_len',
    '	national_character_string_type : _NCHAR _	(129)',
    '	national_character_string_type : _NCHAR _ _VARYING',
    '',
    '	left_paren	shift 944',
    '	_VARYING	shift 959',
    '	$end	reduce 129',
    '	identifier_body	reduce 129',
    '	delimited_identifier	reduce 129',
    '	right_paren	reduce 129',
    '	comma	reduce 129',
    '	semicolon	reduce 129',
    '	underscore	reduce 129',
    '	_ALTER	reduce 129',
    '	_CHECK	reduce 129',
    '	_COLLATE	reduce 129',
    '	_COMMIT	reduce 129',
    '	_CONNECT	reduce 129',
    '	_CONSTRAINT	reduce 129',
    '	_CREATE	reduce 129',
    '	_DECLARE	reduce 129',
    '	_DEFAULT	reduce 129',
    '	_DELETE	reduce 129',
    '	_DISCONNECT	reduce 129',
    '	_DROP	reduce 129',
    '	_GRANT	reduce 129',
    '	_INSERT	reduce 129',
    '	_NOT	reduce 129',
    '	_PRIMARY	reduce 129',
    '	_REFERENCES	reduce 129',
    '	_REVOKE	reduce 129',
    '	_ROLLBACK	reduce 129',
    '	_SELECT	reduce 129',
    '	_SET	reduce 129',
    '	_TABLE	reduce 129',
    '	_UNIQUE	reduce 129',
    '	_UPDATE	reduce 129',
    '	_VALUES	reduce 129',
    '	.	error',
    '',
    '	character_string_type_len	goto 958',
    '',
    'state 740:',
    '',
    '	*** conflicts:',
    '',
    '	shift 951, reduce 145 on left_paren',
    '',
    '	exact_numeric_type : _NUMERIC _ numeric_precision_scale_opt',
    '	numeric_precision_scale_opt : _	(145)',
    '',
    '	left_paren	shift 951',
    '	$end	reduce 145',
    '	identifier_body	reduce 145',
    '	delimited_identifier	reduce 145',
    '	right_paren	reduce 145',
    '	comma	reduce 145',
    '	semicolon	reduce 145',
    '	underscore	reduce 145',
    '	_ALTER	reduce 145',
    '	_CHECK	reduce 145',
    '	_COLLATE	reduce 145',
    '	_COMMIT	reduce 145',
    '	_CONNECT	reduce 145',
    '	_CONSTRAINT	reduce 145',
    '	_CREATE	reduce 145',
    '	_DECLARE	reduce 145',
    '	_DEFAULT	reduce 145',
    '	_DELETE	reduce 145',
    '	_DISCONNECT	reduce 145',
    '	_DROP	reduce 145',
    '	_GRANT	reduce 145',
    '	_INSERT	reduce 145',
    '	_NOT	reduce 145',
    '	_PRIMARY	reduce 145',
    '	_REFERENCES	reduce 145',
    '	_REVOKE	reduce 145',
    '	_ROLLBACK	reduce 145',
    '	_SELECT	reduce 145',
    '	_SET	reduce 145',
    '	_TABLE	reduce 145',
    '	_UNIQUE	reduce 145',
    '	_UPDATE	reduce 145',
    '	_VALUES	reduce 145',
    '	.	error',
    '',
    '	numeric_precision_scale_opt	goto 960',
    '',
    'state 741:',
    '',
    '	approximate_numeric_type : _REAL _	(152)',
    '',
    '	.	reduce 152',
    '',
    'state 742:',
    '',
    '	exact_numeric_type : _SMALLINT _	(144)',
    '',
    '	.	reduce 144',
    '',
    'state 743:',
    '',
    '	*** conflicts:',
    '',
    '	shift 962, reduce 159 on left_paren',
    '',
    '	datetime_type : _TIME _ time_precision_opt tz_opt',
    '	time_precision_opt : _	(159)',
    '',
    '	left_paren	shift 962',
    '	$end	reduce 159',
    '	identifier_body	reduce 159',
    '	delimited_identifier	reduce 159',
    '	right_paren	reduce 159',
    '	comma	reduce 159',
    '	semicolon	reduce 159',
    '	underscore	reduce 159',
    '	_ALTER	reduce 159',
    '	_CHECK	reduce 159',
    '	_COLLATE	reduce 159',
    '	_COMMIT	reduce 159',
    '	_CONNECT	reduce 159',
    '	_CONSTRAINT	reduce 159',
    '	_CREATE	reduce 159',
    '	_DECLARE	reduce 159',
    '	_DEFAULT	reduce 159',
    '	_DELETE	reduce 159',
    '	_DISCONNECT	reduce 159',
    '	_DROP	reduce 159',
    '	_GRANT	reduce 159',
    '	_INSERT	reduce 159',
    '	_NOT	reduce 159',
    '	_PRIMARY	reduce 159',
    '	_REFERENCES	reduce 159',
    '	_REVOKE	reduce 159',
    '	_ROLLBACK	reduce 159',
    '	_SELECT	reduce 159',
    '	_SET	reduce 159',
    '	_TABLE	reduce 159',
    '	_UNIQUE	reduce 159',
    '	_UPDATE	reduce 159',
    '	_VALUES	reduce 159',
    '	_WITH	reduce 159',
    '	.	error',
    '',
    '	time_precision_opt	goto 961',
    '',
    'state 744:',
    '',
    '	*** conflicts:',
    '',
    '	shift 964, reduce 157 on left_paren',
    '',
    '	datetime_type : _TIMESTAMP _ timestamp_precision_opt tz_opt',
    '	timestamp_precision_opt : _	(157)',
    '',
    '	left_paren	shift 964',
    '	$end	reduce 157',
    '	identifier_body	reduce 157',
    '	delimited_identifier	reduce 157',
    '	right_paren	reduce 157',
    '	comma	reduce 157',
    '	semicolon	reduce 157',
    '	underscore	reduce 157',
    '	_ALTER	reduce 157',
    '	_CHECK	reduce 157',
    '	_COLLATE	reduce 157',
    '	_COMMIT	reduce 157',
    '	_CONNECT	reduce 157',
    '	_CONSTRAINT	reduce 157',
    '	_CREATE	reduce 157',
    '	_DECLARE	reduce 157',
    '	_DEFAULT	reduce 157',
    '	_DELETE	reduce 157',
    '	_DISCONNECT	reduce 157',
    '	_DROP	reduce 157',
    '	_GRANT	reduce 157',
    '	_INSERT	reduce 157',
    '	_NOT	reduce 157',
    '	_PRIMARY	reduce 157',
    '	_REFERENCES	reduce 157',
    '	_REVOKE	reduce 157',
    '	_ROLLBACK	reduce 157',
    '	_SELECT	reduce 157',
    '	_SET	reduce 157',
    '	_TABLE	reduce 157',
    '	_UNIQUE	reduce 157',
    '	_UPDATE	reduce 157',
    '	_VALUES	reduce 157',
    '	_WITH	reduce 157',
    '	.	error',
    '',
    '	timestamp_precision_opt	goto 963',
    '',
    'state 745:',
    '',
    '	*** conflicts:',
    '',
    '	shift 944, reduce 118 on left_paren',
    '',
    '	character_string_type : _VARCHAR _ character_string_type_len',
    '	character_string_type : _VARCHAR _	(118)',
    '',
    '	left_paren	shift 944',
    '	$end	reduce 118',
    '	identifier_body	reduce 118',
    '	delimited_identifier	reduce 118',
    '	right_paren	reduce 118',
    '	comma	reduce 118',
    '	semicolon	reduce 118',
    '	underscore	reduce 118',
    '	_ALTER	reduce 118',
    '	_CHARACTER	reduce 118',
    '	_CHECK	reduce 118',
    '	_COLLATE	reduce 118',
    '	_COMMIT	reduce 118',
    '	_CONNECT	reduce 118',
    '	_CONSTRAINT	reduce 118',
    '	_CREATE	reduce 118',
    '	_DECLARE	reduce 118',
    '	_DEFAULT	reduce 118',
    '	_DELETE	reduce 118',
    '	_DISCONNECT	reduce 118',
    '	_DROP	reduce 118',
    '	_GRANT	reduce 118',
    '	_INSERT	reduce 118',
    '	_NOT	reduce 118',
    '	_PRIMARY	reduce 118',
    '	_REFERENCES	reduce 118',
    '	_REVOKE	reduce 118',
    '	_ROLLBACK	reduce 118',
    '	_SELECT	reduce 118',
    '	_SET	reduce 118',
    '	_TABLE	reduce 118',
    '	_UNIQUE	reduce 118',
    '	_UPDATE	reduce 118',
    '	_VALUES	reduce 118',
    '	.	error',
    '',
    '	character_string_type_len	goto 965',
    '',
    'state 746:',
    '',
    '	schema_elements : schema_element _	(632)',
    '',
    '	.	reduce 632',
    '',
    'state 747:',
    '',
    '	*** conflicts:',
    '',
    '	shift 756, reduce 629 on _CREATE',
    '	shift 78, reduce 629 on _GRANT',
    '',
    '	schema_definition : _CREATE _SCHEMA schema_name_clause schema_character_set_specification_opt schema_elements _	(629)',
    '	schema_elements : schema_elements _ schema_element',
    '',
    '	_CREATE	shift 756',
    '	_GRANT	shift 78',
    '	$end	reduce 629',
    '	identifier_body	reduce 629',
    '	delimited_identifier	reduce 629',
    '	left_paren	reduce 629',
    '	semicolon	reduce 629',
    '	underscore	reduce 629',
    '	_ALTER	reduce 629',
    '	_COMMIT	reduce 629',
    '	_CONNECT	reduce 629',
    '	_DECLARE	reduce 629',
    '	_DELETE	reduce 629',
    '	_DISCONNECT	reduce 629',
    '	_DROP	reduce 629',
    '	_INSERT	reduce 629',
    '	_REVOKE	reduce 629',
    '	_ROLLBACK	reduce 629',
    '	_SELECT	reduce 629',
    '	_SET	reduce 629',
    '	_TABLE	reduce 629',
    '	_UPDATE	reduce 629',
    '	_VALUES	reduce 629',
    '	.	error',
    '',
    '	schema_element	goto 966',
    '	assertion_definition	goto 748',
    '	translation_definition	goto 749',
    '	collation_definition	goto 750',
    '	character_set_definition	goto 751',
    '	domain_definition	goto 752',
    '	grant_statement	goto 753',
    '	view_definition	goto 754',
    '	table_definition	goto 755',
    '',
    'state 748:',
    '',
    '	schema_element : assertion_definition _	(643)',
    '',
    '	.	reduce 643',
    '',
    'state 749:',
    '',
    '	schema_element : translation_definition _	(646)',
    '',
    '	.	reduce 646',
    '',
    'state 750:',
    '',
    '	schema_element : collation_definition _	(645)',
    '',
    '	.	reduce 645',
    '',
    'state 751:',
    '',
    '	schema_element : character_set_definition _	(644)',
    '',
    '	.	reduce 644',
    '',
    'state 752:',
    '',
    '	schema_element : domain_definition _	(639)',
    '',
    '	.	reduce 639',
    '',
    'state 753:',
    '',
    '	schema_element : grant_statement _	(642)',
    '',
    '	.	reduce 642',
    '',
    'state 754:',
    '',
    '	schema_element : view_definition _	(641)',
    '',
    '	.	reduce 641',
    '',
    'state 755:',
    '',
    '	schema_element : table_definition _	(640)',
    '',
    '	.	reduce 640',
    '',
    'state 756:',
    '',
    '	domain_definition : _CREATE _ _DOMAIN domain_name as_opt data_type default_clause_opt domain_constraint_opt collate_clause_opt',
    '	table_definition : _CREATE _ table_definition_opts _TABLE table_name table_element_list table_commit_opts',
    '	view_definition : _CREATE _ _VIEW table_name view_column_list_opt _AS query_expression view_check_opt',
    '	assertion_definition : _CREATE _ _ASSERTION constraint_name assertion_check constraint_attributes_opt',
    '	character_set_definition : _CREATE _ _CHARACTER _SET character_set_name as_opt character_set_source charset_collation_opt',
    '	collation_definition : _CREATE _ _COLLATION collation_name _FOR character_set_specification _FROM collation_source pad_attribute_opt',
    '	translation_definition : _CREATE _ _TRANSLATION translation_name _FOR source_character_set_specification _TO target_character_set_specification _FROM translation_source',
    '	table_definition_opts : _	(652)',
    '',
    '	_ASSERTION	shift 107',
    '	_CHARACTER	shift 108',
    '	_COLLATION	shift 109',
    '	_DOMAIN	shift 110',
    '	_GLOBAL	shift 111',
    '	_LOCAL	shift 112',
    '	_TRANSLATION	shift 114',
    '	_VIEW	shift 115',
    '	_TABLE	reduce 652',
    '	.	error',
    '',
    '	table_definition_opts	goto 106',
    '',
    'state 757:',
    '',
    '	schema_character_set_specification : _DEFAULT _CHARACTER _ _SET character_set_specification',
    '',
    '	_SET	shift 967',
    '	.	error',
    '',
    'state 758:',
    '',
    '	schema_name_clause : schema_name _AUTHORIZATION schema_authorization_identifier _	(636)',
    '',
    '	.	reduce 636',
    '',
    'state 759:',
    '',
    '	schema_name : identifier period identifier _	(36)',
    '',
    '	.	reduce 36',
    '',
    'state 760:',
    '',
    '	translation_definition : _CREATE _TRANSLATION translation_name _FOR source_character_set_specification _ _TO target_character_set_specification _FROM translation_source',
    '',
    '	_TO	shift 968',
    '	.	error',
    '',
    'state 761:',
    '',
    '	source_character_set_specification : character_set_specification _	(719)',
    '',
    '	.	reduce 719',
    '',
    'state 762:',
    '',
    '	view_definition : _CREATE _VIEW table_name view_column_list_opt _AS _ query_expression view_check_opt',
    '',
    '	left_paren	shift 68',
    '	_SELECT	shift 83',
    '	_TABLE	shift 85',
    '	_VALUES	shift 87',
    '	.	error',
    '',
    '	explicit_table	goto 49',
    '	table_value_constructor	goto 50',
    '	query_specification	goto 51',
    '	table_subquery	goto 52',
    '	simple_table	goto 53',
    '	non_join_query_primary	goto 54',
    '	query_term	goto 55',
    '	non_join_query_term	goto 56',
    '	query_expression	goto 969',
    '',
    'state 763:',
    '',
    '	view_column_list_opt : left_paren view_column_list _ right_paren',
    '',
    '	right_paren	shift 970',
    '	.	error',
    '',
    'state 764:',
    '',
    '	column_name_list : column_name_list _ comma column_name',
    '	view_column_list : column_name_list _	(665)',
    '',
    '	comma	shift 786',
    '	right_paren	reduce 665',
    '	.	error',
    '',
    'state 765:',
    '',
    '	temporary_table_declaration : _DECLARE _LOCAL _TEMPORARY _TABLE qualified_local_table_name _ table_element_list temporary_table_declaration_opt',
    '',
    '	left_paren	shift 709',
    '	.	error',
    '',
    '	table_element_list	goto 971',
    '',
    'state 766:',
    '',
    '	where_clause : _WHERE search_condition _	(423)',
    '	search_condition : search_condition _ _OR boolean_term',
    '',
    '	_OR	shift 857',
    '	$end	reduce 423',
    '	identifier_body	reduce 423',
    '	delimited_identifier	reduce 423',
    '	left_paren	reduce 423',
    '	right_paren	reduce 423',
    '	semicolon	reduce 423',
    '	underscore	reduce 423',
    '	_ALTER	reduce 423',
    '	_COMMIT	reduce 423',
    '	_CONNECT	reduce 423',
    '	_CREATE	reduce 423',
    '	_DECLARE	reduce 423',
    '	_DELETE	reduce 423',
    '	_DISCONNECT	reduce 423',
    '	_DROP	reduce 423',
    '	_EXCEPT	reduce 423',
    '	_FOR	reduce 423',
    '	_GRANT	reduce 423',
    '	_GROUP	reduce 423',
    '	_HAVING	reduce 423',
    '	_INSERT	reduce 423',
    '	_INTERSECT	reduce 423',
    '	_ORDER	reduce 423',
    '	_REVOKE	reduce 423',
    '	_ROLLBACK	reduce 423',
    '	_SELECT	reduce 423',
    '	_SET	reduce 423',
    '	_TABLE	reduce 423',
    '	_UNION	reduce 423',
    '	_UPDATE	reduce 423',
    '	_VALUES	reduce 423',
    '	_WITH	reduce 423',
    '	.	error',
    '',
    'state 767:',
    '',
    '	signed_integer : sign unsigned_integer _	(14)',
    '	unsigned_integer : unsigned_integer _ digit',
    '',
    '	digit	shift 331',
    '	$end	reduce 14',
    '	identifier_body	reduce 14',
    '	delimited_identifier	reduce 14',
    '	not_equals_operator	reduce 14',
    '	greater_than_or_equals_operator	reduce 14',
    '	less_than_or_equals_operator	reduce 14',
    '	concatenation_operator	reduce 14',
    '	left_paren	reduce 14',
    '	right_paren	reduce 14',
    '	asterisk	reduce 14',
    '	plus_sign	reduce 14',
    '	comma	reduce 14',
    '	minus_sign	reduce 14',
    '	solidus	reduce 14',
    '	semicolon	reduce 14',
    '	less_than_operator	reduce 14',
    '	equals_operator	reduce 14',
    '	greater_than_operator	reduce 14',
    '	underscore	reduce 14',
    '	_ALTER	reduce 14',
    '	_AND	reduce 14',
    '	_AS	reduce 14',
    '	_AT	reduce 14',
    '	_BETWEEN	reduce 14',
    '	_CHECK	reduce 14',
    '	_COLLATE	reduce 14',
    '	_COMMIT	reduce 14',
    '	_CONNECT	reduce 14',
    '	_CONSTRAINT	reduce 14',
    '	_CREATE	reduce 14',
    '	_CROSS	reduce 14',
    '	_DAY	reduce 14',
    '	_DECLARE	reduce 14',
    '	_DELETE	reduce 14',
    '	_DISCONNECT	reduce 14',
    '	_DROP	reduce 14',
    '	_ELSE	reduce 14',
    '	_END	reduce 14',
    '	_ESCAPE	reduce 14',
    '	_EXCEPT	reduce 14',
    '	_FOR	reduce 14',
    '	_FROM	reduce 14',
    '	_FULL	reduce 14',
    '	_GRANT	reduce 14',
    '	_GROUP	reduce 14',
    '	_HAVING	reduce 14',
    '	_HOUR	reduce 14',
    '	_IN	reduce 14',
    '	_INNER	reduce 14',
    '	_INSERT	reduce 14',
    '	_INTERSECT	reduce 14',
    '	_INTO	reduce 14',
    '	_IS	reduce 14',
    '	_JOIN	reduce 14',
    '	_LEFT	reduce 14',
    '	_LIKE	reduce 14',
    '	_MATCH	reduce 14',
    '	_MINUTE	reduce 14',
    '	_MONTH	reduce 14',
    '	_NATURAL	reduce 14',
    '	_NOT	reduce 14',
    '	_OR	reduce 14',
    '	_ORDER	reduce 14',
    '	_OVERLAPS	reduce 14',
    '	_PRIMARY	reduce 14',
    '	_REFERENCES	reduce 14',
    '	_REVOKE	reduce 14',
    '	_RIGHT	reduce 14',
    '	_ROLLBACK	reduce 14',
    '	_SECOND	reduce 14',
    '	_SELECT	reduce 14',
    '	_SET	reduce 14',
    '	_TABLE	reduce 14',
    '	_THEN	reduce 14',
    '	_UNION	reduce 14',
    '	_UNIQUE	reduce 14',
    '	_UPDATE	reduce 14',
    '	_USER	reduce 14',
    '	_USING	reduce 14',
    '	_VALUES	reduce 14',
    '	_WHEN	reduce 14',
    '	_WHERE	reduce 14',
    '	_WITH	reduce 14',
    '	_YEAR	reduce 14',
    '	.	error',
    '',
    'state 768:',
    '',
    '	date_string : quote date_value quote _	(43)',
    '',
    '	.	reduce 43',
    '',
    'state 769:',
    '',
    '	date_value : unsigned_integer minus_sign _ unsigned_integer minus_sign unsigned_integer',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	unsigned_integer	goto 972',
    '',
    'state 770:',
    '',
    '	interval_literal : _INTERVAL sign interval_string interval_qualifier _	(218)',
    '',
    '	.	reduce 218',
    '',
    'state 771:',
    '',
    '	interval_string : quote interval_string_literal quote _	(52)',
    '',
    '	.	reduce 52',
    '',
    'state 772:',
    '',
    '	interval_string_literal : unsigned_integer space _ unsigned_integer',
    '	interval_string_literal : unsigned_integer space _ unsigned_integer colon unsigned_integer',
    '	interval_string_literal : unsigned_integer space _ unsigned_integer colon unsigned_integer colon seconds_value',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	unsigned_integer	goto 973',
    '',
    'state 773:',
    '',
    '	interval_string_literal : unsigned_integer minus_sign _ unsigned_integer',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	unsigned_integer	goto 974',
    '',
    'state 774:',
    '',
    '	interval_string_literal : unsigned_integer period _ unsigned_integer',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	unsigned_integer	goto 975',
    '',
    'state 775:',
    '',
    '	interval_string_literal : unsigned_integer colon _ seconds_value',
    '	interval_string_literal : unsigned_integer colon _ unsigned_integer colon seconds_value',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	seconds_value	goto 976',
    '	unsigned_integer	goto 977',
    '',
    'state 776:',
    '',
    '	time_string : quote time_value quote _ quote time_value time_zone_interval quote',
    '',
    '	quote	shift 978',
    '	.	error',
    '',
    'state 777:',
    '',
    '	time_value : unsigned_integer colon _ unsigned_integer colon seconds_value',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	unsigned_integer	goto 979',
    '',
    'state 778:',
    '',
    '	timestamp_string : quote date_value space _ time_value quote',
    '	timestamp_string : quote date_value space _ time_value time_zone_interval quote',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	time_value	goto 980',
    '	unsigned_integer	goto 532',
    '',
    'state 779:',
    '',
    '	object_name : table_opt table_name _	(684)',
    '',
    '	.	reduce 684',
    '',
    'state 780:',
    '',
    '	grant_statement : _GRANT privileges _ON object_name _TO _ grantee_list grant_option',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_PUBLIC	shift 984',
    '	.	error',
    '',
    '	grantee	goto 981',
    '	grantee_list	goto 982',
    '	authorization_identifier	goto 983',
    '	actual_identifier	goto 61',
    '	identifier	goto 472',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 781:',
    '',
    '	object_name : _CHARACTER _SET _ character_set_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	actual_identifier	goto 61',
    '	SQL_language_identifier	goto 96',
    '	identifier	goto 97',
    '	character_set_name	goto 985',
    '	introducer	goto 63',
    '	regular_identifier	goto 100',
    '',
    'state 782:',
    '',
    '	object_name : _COLLATION collation_name _	(686)',
    '',
    '	.	reduce 686',
    '',
    'state 783:',
    '',
    '	object_name : _DOMAIN domain_name _	(685)',
    '',
    '	.	reduce 685',
    '',
    'state 784:',
    '',
    '	object_name : _TRANSLATION translation_name _	(688)',
    '',
    '	.	reduce 688',
    '',
    'state 785:',
    '',
    '	privilege_column_list_opt : left_paren privilege_column_list right_paren _	(682)',
    '',
    '	.	reduce 682',
    '',
    'state 786:',
    '',
    '	column_name_list : column_name_list comma _ column_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	column_name	goto 986',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 787:',
    '',
    '	insert_columns_and_source : left_paren insert_column_list _ right_paren query_expression',
    '',
    '	right_paren	shift 987',
    '	.	error',
    '',
    'state 788:',
    '',
    '	column_name_list : column_name_list _ comma column_name',
    '	insert_column_list : column_name_list _	(814)',
    '',
    '	comma	shift 786',
    '	right_paren	reduce 814',
    '	.	error',
    '',
    'state 789:',
    '',
    '	insert_columns_and_source : _DEFAULT _VALUES _	(813)',
    '',
    '	.	reduce 813',
    '',
    'state 790:',
    '',
    '	module_name_clause : _MODULE _MODULE module_name _MODULE module_character_set_specification _ _MODULE module_name module_character_set_specification',
    '',
    '	_MODULE	shift 988',
    '	.	error',
    '',
    'state 791:',
    '',
    '	module_character_set_specification : _NAMES _ _ARE character_set_specification',
    '',
    '	_ARE	shift 989',
    '	.	error',
    '',
    'state 792:',
    '',
    '	revoke_statement : _REVOKE grant_option_for_opt privileges _ON object_name _ _FROM grantee_list drop_behaviour',
    '',
    '	_FROM	shift 990',
    '	.	error',
    '',
    'state 793:',
    '',
    '	select_list_opt : select_list_opt comma select_sublist _	(369)',
    '',
    '	.	reduce 369',
    '',
    'state 794:',
    '',
    '	table_expression : from_clause where_clause_opt _ group_by_clause_opt having_clause_opt',
    '	group_by_clause_opt : _	(379)',
    '',
    '	_GROUP	shift 993',
    '	$end	reduce 379',
    '	identifier_body	reduce 379',
    '	delimited_identifier	reduce 379',
    '	left_paren	reduce 379',
    '	right_paren	reduce 379',
    '	semicolon	reduce 379',
    '	underscore	reduce 379',
    '	_ALTER	reduce 379',
    '	_COMMIT	reduce 379',
    '	_CONNECT	reduce 379',
    '	_CREATE	reduce 379',
    '	_DECLARE	reduce 379',
    '	_DELETE	reduce 379',
    '	_DISCONNECT	reduce 379',
    '	_DROP	reduce 379',
    '	_EXCEPT	reduce 379',
    '	_FOR	reduce 379',
    '	_GRANT	reduce 379',
    '	_HAVING	reduce 379',
    '	_INSERT	reduce 379',
    '	_INTERSECT	reduce 379',
    '	_ORDER	reduce 379',
    '	_REVOKE	reduce 379',
    '	_ROLLBACK	reduce 379',
    '	_SELECT	reduce 379',
    '	_SET	reduce 379',
    '	_TABLE	reduce 379',
    '	_UNION	reduce 379',
    '	_UPDATE	reduce 379',
    '	_VALUES	reduce 379',
    '	_WITH	reduce 379',
    '	.	error',
    '',
    '	group_by_clause	goto 991',
    '	group_by_clause_opt	goto 992',
    '',
    'state 795:',
    '',
    '	joined_table : qualified_join _	(402)',
    '',
    '	.	reduce 402',
    '',
    'state 796:',
    '',
    '	joined_table : cross_join _	(401)',
    '',
    '	.	reduce 401',
    '',
    'state 797:',
    '',
    '	table_factor : derived_table _ correlation_specification',
    '	table_factor : derived_table _ _AS correlation_specification',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_AS	shift 997',
    '	.	error',
    '',
    '	correlation_specification	goto 994',
    '	correlation_name	goto 995',
    '	actual_identifier	goto 61',
    '	identifier	goto 996',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 798:',
    '',
    '	table_reference : table_factor _	(387)',
    '',
    '	.	reduce 387',
    '',
    'state 799:',
    '',
    '	table_reference : joined_table _	(386)',
    '',
    '	.	reduce 386',
    '',
    'state 800:',
    '',
    '	from_clause_opt : table_reference _	(384)',
    '	cross_join : table_reference _ _CROSS _JOIN table_factor',
    '	qualified_join : table_reference _ _JOIN table_factor join_specification',
    '	qualified_join : table_reference _ _INNER _JOIN table_factor join_specification',
    '	qualified_join : table_reference _ _LEFT outer_opt _JOIN table_factor join_specification',
    '	qualified_join : table_reference _ _RIGHT outer_opt _JOIN table_factor join_specification',
    '	qualified_join : table_reference _ _FULL outer_opt _JOIN table_factor join_specification',
    '	qualified_join : table_reference _ _NATURAL _JOIN table_factor',
    '	qualified_join : table_reference _ _NATURAL _INNER _JOIN table_factor',
    '	qualified_join : table_reference _ _NATURAL _LEFT outer_opt _JOIN table_factor',
    '	qualified_join : table_reference _ _NATURAL _RIGHT outer_opt _JOIN table_factor',
    '	qualified_join : table_reference _ _NATURAL _FULL outer_opt _JOIN table_factor',
    '	qualified_join : table_reference _ _NATURAL _UNION _JOIN table_factor',
    '',
    '	_CROSS	shift 998',
    '	_FULL	shift 999',
    '	_INNER	shift 1000',
    '	_JOIN	shift 1001',
    '	_LEFT	shift 1002',
    '	_NATURAL	shift 1003',
    '	_RIGHT	shift 1004',
    '	$end	reduce 384',
    '	identifier_body	reduce 384',
    '	delimited_identifier	reduce 384',
    '	left_paren	reduce 384',
    '	right_paren	reduce 384',
    '	comma	reduce 384',
    '	semicolon	reduce 384',
    '	underscore	reduce 384',
    '	_ALTER	reduce 384',
    '	_COMMIT	reduce 384',
    '	_CONNECT	reduce 384',
    '	_CREATE	reduce 384',
    '	_DECLARE	reduce 384',
    '	_DELETE	reduce 384',
    '	_DISCONNECT	reduce 384',
    '	_DROP	reduce 384',
    '	_EXCEPT	reduce 384',
    '	_FOR	reduce 384',
    '	_GRANT	reduce 384',
    '	_GROUP	reduce 384',
    '	_HAVING	reduce 384',
    '	_INSERT	reduce 384',
    '	_INTERSECT	reduce 384',
    '	_ORDER	reduce 384',
    '	_REVOKE	reduce 384',
    '	_ROLLBACK	reduce 384',
    '	_SELECT	reduce 384',
    '	_SET	reduce 384',
    '	_TABLE	reduce 384',
    '	_UNION	reduce 384',
    '	_UPDATE	reduce 384',
    '	_VALUES	reduce 384',
    '	_WHERE	reduce 384',
    '	_WITH	reduce 384',
    '	.	error',
    '',
    'state 801:',
    '',
    '	from_clause : _FROM from_clause_opt _	(383)',
    '	from_clause_opt : from_clause_opt _ comma table_reference',
    '',
    '	comma	shift 1005',
    '	$end	reduce 383',
    '	identifier_body	reduce 383',
    '	delimited_identifier	reduce 383',
    '	left_paren	reduce 383',
    '	right_paren	reduce 383',
    '	semicolon	reduce 383',
    '	underscore	reduce 383',
    '	_ALTER	reduce 383',
    '	_COMMIT	reduce 383',
    '	_CONNECT	reduce 383',
    '	_CREATE	reduce 383',
    '	_DECLARE	reduce 383',
    '	_DELETE	reduce 383',
    '	_DISCONNECT	reduce 383',
    '	_DROP	reduce 383',
    '	_EXCEPT	reduce 383',
    '	_FOR	reduce 383',
    '	_GRANT	reduce 383',
    '	_GROUP	reduce 383',
    '	_HAVING	reduce 383',
    '	_INSERT	reduce 383',
    '	_INTERSECT	reduce 383',
    '	_ORDER	reduce 383',
    '	_REVOKE	reduce 383',
    '	_ROLLBACK	reduce 383',
    '	_SELECT	reduce 383',
    '	_SET	reduce 383',
    '	_TABLE	reduce 383',
    '	_UNION	reduce 383',
    '	_UPDATE	reduce 383',
    '	_VALUES	reduce 383',
    '	_WHERE	reduce 383',
    '	_WITH	reduce 383',
    '	.	error',
    '',
    'state 802:',
    '',
    '	derived_table : table_subquery _	(399)',
    '',
    '	.	reduce 399',
    '',
    'state 803:',
    '',
    '	*** conflicts:',
    '',
    '	shift 66, reduce 388 on identifier_body',
    '	shift 67, reduce 388 on delimited_identifier',
    '	shift 69, reduce 388 on underscore',
    '',
    '	table_factor : table_name _	(388)',
    '	table_factor : table_name _ correlation_specification',
    '	table_factor : table_name _ _AS correlation_specification',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_AS	shift 1007',
    '	$end	reduce 388',
    '	left_paren	reduce 388',
    '	right_paren	reduce 388',
    '	comma	reduce 388',
    '	semicolon	reduce 388',
    '	_ALTER	reduce 388',
    '	_COMMIT	reduce 388',
    '	_CONNECT	reduce 388',
    '	_CREATE	reduce 388',
    '	_CROSS	reduce 388',
    '	_DECLARE	reduce 388',
    '	_DELETE	reduce 388',
    '	_DISCONNECT	reduce 388',
    '	_DROP	reduce 388',
    '	_EXCEPT	reduce 388',
    '	_FOR	reduce 388',
    '	_FULL	reduce 388',
    '	_GRANT	reduce 388',
    '	_GROUP	reduce 388',
    '	_HAVING	reduce 388',
    '	_INNER	reduce 388',
    '	_INSERT	reduce 388',
    '	_INTERSECT	reduce 388',
    '	_JOIN	reduce 388',
    '	_LEFT	reduce 388',
    '	_NATURAL	reduce 388',
    '	_ON	reduce 388',
    '	_ORDER	reduce 388',
    '	_REVOKE	reduce 388',
    '	_RIGHT	reduce 388',
    '	_ROLLBACK	reduce 388',
    '	_SELECT	reduce 388',
    '	_SET	reduce 388',
    '	_TABLE	reduce 388',
    '	_UNION	reduce 388',
    '	_UPDATE	reduce 388',
    '	_USING	reduce 388',
    '	_VALUES	reduce 388',
    '	_WHERE	reduce 388',
    '	_WITH	reduce 388',
    '	.	error',
    '',
    '	correlation_specification	goto 1006',
    '	correlation_name	goto 995',
    '	actual_identifier	goto 61',
    '	identifier	goto 996',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 804:',
    '',
    '	table_subquery : left_paren _ query_expression right_paren',
    '	joined_table : left_paren _ joined_table right_paren',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	left_paren	shift 804',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	_SELECT	shift 83',
    '	_TABLE	shift 85',
    '	_VALUES	shift 87',
    '	.	error',
    '',
    '	qualified_join	goto 795',
    '	cross_join	goto 796',
    '	derived_table	goto 797',
    '	table_factor	goto 798',
    '	joined_table	goto 1008',
    '	table_reference	goto 1009',
    '	explicit_table	goto 49',
    '	table_value_constructor	goto 50',
    '	query_specification	goto 51',
    '	table_subquery	goto 1010',
    '	simple_table	goto 53',
    '	non_join_query_primary	goto 54',
    '	query_term	goto 55',
    '	non_join_query_term	goto 56',
    '	query_expression	goto 101',
    '	table_name	goto 803',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 805:',
    '',
    '	as_clause : _AS column_name _	(375)',
    '',
    '	.	reduce 375',
    '',
    'state 806:',
    '',
    '	qualified_name : identifier period identifier _	(188)',
    '	qualified_name : identifier period identifier _ period identifier',
    '	qualified_name_trail_asterisk : identifier period identifier _ period asterisk',
    '	qualified_name_trail_asterisk : identifier period identifier _ period identifier period asterisk',
    '',
    '	period	shift 1011',
    '	identifier_body	reduce 188',
    '	delimited_identifier	reduce 188',
    '	concatenation_operator	reduce 188',
    '	asterisk	reduce 188',
    '	plus_sign	reduce 188',
    '	comma	reduce 188',
    '	minus_sign	reduce 188',
    '	solidus	reduce 188',
    '	underscore	reduce 188',
    '	_AS	reduce 188',
    '	_AT	reduce 188',
    '	_COLLATE	reduce 188',
    '	_DAY	reduce 188',
    '	_FROM	reduce 188',
    '	_HOUR	reduce 188',
    '	_INTO	reduce 188',
    '	_MINUTE	reduce 188',
    '	_MONTH	reduce 188',
    '	_SECOND	reduce 188',
    '	_YEAR	reduce 188',
    '	.	error',
    '',
    'state 807:',
    '',
    '	qualified_name_trail_asterisk : identifier period asterisk _	(190)',
    '',
    '	.	reduce 190',
    '',
    'state 808:',
    '',
    '	constraint_name_list_some : constraint_name_list_some comma constraint_name _	(847)',
    '',
    '	.	reduce 847',
    '',
    'state 809:',
    '',
    '	transaction_mode_list : transaction_mode_list comma transaction_mode _	(828)',
    '',
    '	.	reduce 828',
    '',
    'state 810:',
    '',
    '	diagnostics_size : _DIAGNOSTICS _SIZE number_of_conditions _	(840)',
    '',
    '	.	reduce 840',
    '',
    'state 811:',
    '',
    '	number_of_conditions : simple_value_specification _	(841)',
    '',
    '	.	reduce 841',
    '',
    'state 812:',
    '',
    '	isolation_level : _ISOLATION _LEVEL level_of_isolation _	(832)',
    '',
    '	.	reduce 832',
    '',
    'state 813:',
    '',
    '	level_of_isolation : _READ _ _UNCOMMITTED',
    '	level_of_isolation : _READ _ _COMMITTED',
    '',
    '	_COMMITTED	shift 1012',
    '	_UNCOMMITTED	shift 1013',
    '	.	error',
    '',
    'state 814:',
    '',
    '	level_of_isolation : _REPEATABLE _ _READ',
    '',
    '	_READ	shift 1014',
    '	.	error',
    '',
    'state 815:',
    '',
    '	level_of_isolation : _SERIALIZABLE _	(836)',
    '',
    '	.	reduce 836',
    '',
    'state 816:',
    '',
    '	level_of_isolation : _SNAPSHOT _	(837)',
    '',
    '	.	reduce 837',
    '',
    'state 817:',
    '',
    '	qualified_name : identifier period identifier period _ identifier',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	actual_identifier	goto 61',
    '	identifier	goto 1015',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 818:',
    '',
    '	set_clause : object_column equals_operator _ update_source',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	update_source	goto 1016',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 1017',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 819:',
    '',
    '	update_statement__searched : _UPDATE table_name _SET set_clause_list where_clause_opt _	(821)',
    '',
    '	.	reduce 821',
    '',
    'state 820:',
    '',
    '	set_clause_list : set_clause_list comma _ set_clause',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	object_column	goto 583',
    '	set_clause	goto 1018',
    '	column_name	goto 586',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 821:',
    '',
    '	char_length_expression : char_length_specifier left_paren expression right_paren _	(507)',
    '',
    '	.	reduce 507',
    '',
    'state 822:',
    '',
    '	set_quantifier_args : set_quantifier expression _	(341)',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	right_paren	reduce 341',
    '	.	error',
    '',
    'state 823:',
    '',
    '	general_set_function : set_function_type left_paren set_quantifier_args right_paren _	(338)',
    '',
    '	.	reduce 338',
    '',
    'state 824:',
    '',
    '	start_field : non_second_datetime_field left_paren precision _ right_paren',
    '',
    '	right_paren	shift 1019',
    '	.	error',
    '',
    'state 825:',
    '',
    '	unsigned_integer : unsigned_integer _ digit',
    '	precision : unsigned_integer _	(148)',
    '',
    '	digit	shift 331',
    '	right_paren	reduce 148',
    '	comma	reduce 148',
    '	.	error',
    '',
    'state 826:',
    '',
    '	end_field : non_second_datetime_field _	(178)',
    '',
    '	.	reduce 178',
    '',
    'state 827:',
    '',
    '	interval_qualifier : start_field _TO end_field _	(168)',
    '',
    '	.	reduce 168',
    '',
    'state 828:',
    '',
    '	*** conflicts:',
    '',
    '	shift 1020, reduce 179 on left_paren',
    '',
    '	end_field : _SECOND _	(179)',
    '	end_field : _SECOND _ left_paren precision right_paren',
    '',
    '	left_paren	shift 1020',
    '	$end	reduce 179',
    '	identifier_body	reduce 179',
    '	delimited_identifier	reduce 179',
    '	not_equals_operator	reduce 179',
    '	greater_than_or_equals_operator	reduce 179',
    '	less_than_or_equals_operator	reduce 179',
    '	concatenation_operator	reduce 179',
    '	right_paren	reduce 179',
    '	asterisk	reduce 179',
    '	plus_sign	reduce 179',
    '	comma	reduce 179',
    '	minus_sign	reduce 179',
    '	solidus	reduce 179',
    '	semicolon	reduce 179',
    '	less_than_operator	reduce 179',
    '	equals_operator	reduce 179',
    '	greater_than_operator	reduce 179',
    '	underscore	reduce 179',
    '	_ALTER	reduce 179',
    '	_AND	reduce 179',
    '	_AS	reduce 179',
    '	_AT	reduce 179',
    '	_BETWEEN	reduce 179',
    '	_CHECK	reduce 179',
    '	_COLLATE	reduce 179',
    '	_COMMIT	reduce 179',
    '	_CONNECT	reduce 179',
    '	_CONSTRAINT	reduce 179',
    '	_CREATE	reduce 179',
    '	_CROSS	reduce 179',
    '	_DAY	reduce 179',
    '	_DECLARE	reduce 179',
    '	_DEFAULT	reduce 179',
    '	_DELETE	reduce 179',
    '	_DISCONNECT	reduce 179',
    '	_DROP	reduce 179',
    '	_ELSE	reduce 179',
    '	_END	reduce 179',
    '	_ESCAPE	reduce 179',
    '	_EXCEPT	reduce 179',
    '	_FOR	reduce 179',
    '	_FROM	reduce 179',
    '	_FULL	reduce 179',
    '	_GRANT	reduce 179',
    '	_GROUP	reduce 179',
    '	_HAVING	reduce 179',
    '	_HOUR	reduce 179',
    '	_IN	reduce 179',
    '	_INNER	reduce 179',
    '	_INSERT	reduce 179',
    '	_INTERSECT	reduce 179',
    '	_INTO	reduce 179',
    '	_IS	reduce 179',
    '	_JOIN	reduce 179',
    '	_LEFT	reduce 179',
    '	_LIKE	reduce 179',
    '	_MATCH	reduce 179',
    '	_MINUTE	reduce 179',
    '	_MONTH	reduce 179',
    '	_NATURAL	reduce 179',
    '	_NOT	reduce 179',
    '	_OR	reduce 179',
    '	_ORDER	reduce 179',
    '	_OVERLAPS	reduce 179',
    '	_PRIMARY	reduce 179',
    '	_REFERENCES	reduce 179',
    '	_REVOKE	reduce 179',
    '	_RIGHT	reduce 179',
    '	_ROLLBACK	reduce 179',
    '	_SECOND	reduce 179',
    '	_SELECT	reduce 179',
    '	_SET	reduce 179',
    '	_TABLE	reduce 179',
    '	_THEN	reduce 179',
    '	_UNION	reduce 179',
    '	_UNIQUE	reduce 179',
    '	_UPDATE	reduce 179',
    '	_USER	reduce 179',
    '	_USING	reduce 179',
    '	_VALUES	reduce 179',
    '	_WHEN	reduce 179',
    '	_WHERE	reduce 179',
    '	_WITH	reduce 179',
    '	_YEAR	reduce 179',
    '	.	error',
    '',
    'state 829:',
    '',
    '	time_zone_specifier : _TIME _ZONE _ expression',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 1021',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 830:',
    '',
    '	single_datetime_field_opt : left_paren interval_leading_field_precision _ single_datetime_field_opt2 right_paren',
    '	single_datetime_field_opt2 : _	(184)',
    '',
    '	comma	shift 1023',
    '	right_paren	reduce 184',
    '	.	error',
    '',
    '	single_datetime_field_opt2	goto 1022',
    '',
    'state 831:',
    '',
    '	unsigned_integer : unsigned_integer _ digit',
    '	interval_leading_field_precision : unsigned_integer _	(177)',
    '',
    '	digit	shift 331',
    '	right_paren	reduce 177',
    '	comma	reduce 177',
    '	.	error',
    '',
    'state 832:',
    '',
    '	row_value_constructor_list : row_value_constructor_list comma expression _	(515)',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	right_paren	reduce 515',
    '	comma	reduce 515',
    '	.	error',
    '',
    'state 833:',
    '',
    '	bit_length_expression : _BIT_LENGTH left_paren expression right_paren _	(511)',
    '',
    '	.	reduce 511',
    '',
    'state 834:',
    '',
    '	searched_case : _CASE searched_when_clause else_clause_opt _END _	(457)',
    '',
    '	.	reduce 457',
    '',
    'state 835:',
    '',
    '	else_clause : _ELSE result _	(456)',
    '',
    '	.	reduce 456',
    '',
    'state 836:',
    '',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	result : expression _	(455)',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	_ELSE	reduce 455',
    '	_END	reduce 455',
    '	.	error',
    '',
    'state 837:',
    '',
    '	simple_case : _CASE case_operand simple_when_clause else_clause_opt _ _END',
    '',
    '	_END	shift 1024',
    '	.	error',
    '',
    'state 838:',
    '',
    '	simple_when_clause : _WHEN when_operand _ _THEN result',
    '',
    '	_THEN	shift 1025',
    '	.	error',
    '',
    'state 839:',
    '',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	when_operand : expression _	(454)',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	_THEN	reduce 454',
    '	.	error',
    '',
    'state 840:',
    '',
    '	overlaps_predicate : row_value_constructor_1 _OVERLAPS _ row_value_constructor_2',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 248',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	row_value_constructor_2	goto 1026',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 238',
    '	row_value_constructor	goto 1027',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 841:',
    '',
    '	like_predicate : expression _LIKE _ pattern like_predicate_escape_opt',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	pattern	goto 1028',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 1029',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 842:',
    '',
    '	like_predicate : expression _NOT _ _LIKE pattern like_predicate_escape_opt',
    '',
    '	_LIKE	shift 1030',
    '	.	error',
    '',
    'state 843:',
    '',
    '	comparison_predicate : row_value_constructor comp_op _ row_value_constructor',
    '	quantified_comparison_predicate : row_value_constructor comp_op _ quantifier table_subquery',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 248',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_ALL	shift 1035',
    '	_ANY	shift 1036',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SOME	shift 1037',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	some	goto 1031',
    '	all	goto 1032',
    '	quantifier	goto 1033',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 238',
    '	row_value_constructor	goto 1034',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 844:',
    '',
    '	comp_op : not_equals_operator _	(517)',
    '',
    '	.	reduce 517',
    '',
    'state 845:',
    '',
    '	comp_op : greater_than_or_equals_operator _	(521)',
    '',
    '	.	reduce 521',
    '',
    'state 846:',
    '',
    '	comp_op : less_than_or_equals_operator _	(520)',
    '',
    '	.	reduce 520',
    '',
    'state 847:',
    '',
    '	comp_op : less_than_operator _	(518)',
    '',
    '	.	reduce 518',
    '',
    'state 848:',
    '',
    '	comp_op : equals_operator _	(516)',
    '',
    '	.	reduce 516',
    '',
    'state 849:',
    '',
    '	comp_op : greater_than_operator _	(519)',
    '',
    '	.	reduce 519',
    '',
    'state 850:',
    '',
    '	between_predicate : row_value_constructor _BETWEEN _ row_value_constructor _AND row_value_constructor',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 248',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 238',
    '	row_value_constructor	goto 1038',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 851:',
    '',
    '	in_predicate : row_value_constructor _IN _ in_predicate_value',
    '',
    '	left_paren	shift 1041',
    '	.	error',
    '',
    '	in_predicate_value	goto 1039',
    '	table_subquery	goto 1040',
    '',
    'state 852:',
    '',
    '	null_predicate : row_value_constructor _IS _ _NULL',
    '	null_predicate : row_value_constructor _IS _ _NOT _NULL',
    '',
    '	_NOT	shift 1042',
    '	_NULL	shift 1043',
    '	.	error',
    '',
    'state 853:',
    '',
    '	match_predicate : row_value_constructor _MATCH _ unique_opt partial_full_opt table_subquery',
    '	unique_opt : _	(547)',
    '',
    '	_UNIQUE	shift 1045',
    '	left_paren	reduce 547',
    '	_FULL	reduce 547',
    '	_PARTIAL	reduce 547',
    '	.	error',
    '',
    '	unique_opt	goto 1044',
    '',
    'state 854:',
    '',
    '	between_predicate : row_value_constructor _NOT _ _BETWEEN row_value_constructor _AND row_value_constructor',
    '	in_predicate : row_value_constructor _NOT _ _IN in_predicate_value',
    '',
    '	_BETWEEN	shift 1046',
    '	_IN	shift 1047',
    '	.	error',
    '',
    'state 855:',
    '',
    '	boolean_test : boolean_primary _IS _ truth_value',
    '	boolean_test : boolean_primary _IS _ _NOT truth_value',
    '',
    '	_FALSE	shift 1049',
    '	_NOT	shift 1050',
    '	_TRUE	shift 1051',
    '	_UNKNOWN	shift 1052',
    '	.	error',
    '',
    '	truth_value	goto 1048',
    '',
    'state 856:',
    '',
    '	boolean_term : boolean_term _AND _ boolean_factor',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 636',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXISTS	shift 637',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NOT	shift 638',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UNIQUE	shift 639',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	row_value_constructor_1	goto 617',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 618',
    '	row_value_constructor	goto 619',
    '	overlaps_predicate	goto 620',
    '	match_predicate	goto 621',
    '	unique_predicate	goto 622',
    '	exists_predicate	goto 623',
    '	quantified_comparison_predicate	goto 624',
    '	null_predicate	goto 625',
    '	like_predicate	goto 626',
    '	in_predicate	goto 627',
    '	between_predicate	goto 628',
    '	comparison_predicate	goto 629',
    '	predicate	goto 630',
    '	boolean_primary	goto 631',
    '	boolean_test	goto 632',
    '	boolean_factor	goto 1053',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 857:',
    '',
    '	search_condition : search_condition _OR _ boolean_term',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 636',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXISTS	shift 637',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NOT	shift 638',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UNIQUE	shift 639',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	row_value_constructor_1	goto 617',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 618',
    '	row_value_constructor	goto 619',
    '	overlaps_predicate	goto 620',
    '	match_predicate	goto 621',
    '	unique_predicate	goto 622',
    '	exists_predicate	goto 623',
    '	quantified_comparison_predicate	goto 624',
    '	null_predicate	goto 625',
    '	like_predicate	goto 626',
    '	in_predicate	goto 627',
    '	between_predicate	goto 628',
    '	comparison_predicate	goto 629',
    '	predicate	goto 630',
    '	boolean_primary	goto 631',
    '	boolean_test	goto 632',
    '	boolean_factor	goto 633',
    '	boolean_term	goto 1054',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 858:',
    '',
    '	searched_when_clause : _WHEN search_condition _THEN _ result',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	result	goto 1055',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 836',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 859:',
    '',
    '	*** conflicts:',
    '',
    '	shift 609, reduce 514 on right_paren',
    '	shift 842, reduce 288 on _NOT',
    '',
    '	primary_expression : left_paren expression _ right_paren',
    '	row_value_constructor : expression _	(288)',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	row_value_constructor_list : expression _	(514)',
    '	like_predicate : expression _ _LIKE pattern like_predicate_escape_opt',
    '	like_predicate : expression _ _NOT _LIKE pattern like_predicate_escape_opt',
    '',
    '	concatenation_operator	shift 421',
    '	right_paren	shift 609',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	_LIKE	shift 841',
    '	_NOT	shift 842',
    '	not_equals_operator	reduce 288',
    '	greater_than_or_equals_operator	reduce 288',
    '	less_than_or_equals_operator	reduce 288',
    '	less_than_operator	reduce 288',
    '	equals_operator	reduce 288',
    '	greater_than_operator	reduce 288',
    '	_BETWEEN	reduce 288',
    '	_IN	reduce 288',
    '	_IS	reduce 288',
    '	_MATCH	reduce 288',
    '	_OVERLAPS	reduce 288',
    '	comma	reduce 514',
    '	.	error',
    '',
    'state 860:',
    '',
    '	boolean_primary : left_paren search_condition _ right_paren',
    '	search_condition : search_condition _ _OR boolean_term',
    '',
    '	right_paren	shift 1056',
    '	_OR	shift 857',
    '	.	error',
    '',
    'state 861:',
    '',
    '	boolean_primary : left_paren _ search_condition right_paren',
    '	row_value_constructor : left_paren _ row_value_constructor_list right_paren',
    '	primary_expression : left_paren _ expression right_paren',
    '	scalar_subquery : left_paren _ subquery right_paren',
    '	table_subquery : left_paren _ query_expression right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 861',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXISTS	shift 637',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NOT	shift 638',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SELECT	shift 83',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TABLE	shift 85',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UNIQUE	shift 639',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_VALUES	shift 87',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	row_value_constructor_1	goto 617',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	explicit_table	goto 49',
    '	table_value_constructor	goto 50',
    '	query_specification	goto 51',
    '	table_subquery	goto 52',
    '	simple_table	goto 53',
    '	non_join_query_primary	goto 54',
    '	query_term	goto 55',
    '	non_join_query_term	goto 56',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	query_expression	goto 610',
    '	subquery	goto 426',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	row_value_constructor_list	goto 427',
    '	expression	goto 859',
    '	row_value_constructor	goto 619',
    '	overlaps_predicate	goto 620',
    '	match_predicate	goto 621',
    '	unique_predicate	goto 622',
    '	exists_predicate	goto 623',
    '	quantified_comparison_predicate	goto 624',
    '	null_predicate	goto 625',
    '	like_predicate	goto 626',
    '	in_predicate	goto 627',
    '	between_predicate	goto 628',
    '	comparison_predicate	goto 629',
    '	predicate	goto 630',
    '	boolean_primary	goto 631',
    '	boolean_test	goto 632',
    '	boolean_factor	goto 633',
    '	boolean_term	goto 634',
    '	search_condition	goto 860',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 862:',
    '',
    '	exists_predicate : _EXISTS table_subquery _	(544)',
    '',
    '	.	reduce 544',
    '',
    'state 863:',
    '',
    '	boolean_factor : _NOT boolean_test _	(271)',
    '',
    '	.	reduce 271',
    '',
    'state 864:',
    '',
    '	unique_predicate : _UNIQUE table_subquery _	(545)',
    '',
    '	.	reduce 545',
    '',
    'state 865:',
    '',
    '	cast_specification : _CAST left_paren cast_operand _AS _ cast_target right_paren',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_BIT	shift 727',
    '	_CHAR	shift 728',
    '	_CHARACTER	shift 729',
    '	_DATE	shift 730',
    '	_DEC	shift 731',
    '	_DECIMAL	shift 732',
    '	_DOUBLE	shift 733',
    '	_FLOAT	shift 734',
    '	_INT	shift 735',
    '	_INTEGER	shift 736',
    '	_INTERVAL	shift 737',
    '	_NATIONAL	shift 738',
    '	_NCHAR	shift 739',
    '	_NUMERIC	shift 740',
    '	_REAL	shift 741',
    '	_SMALLINT	shift 742',
    '	_TIME	shift 743',
    '	_TIMESTAMP	shift 744',
    '	_VARCHAR	shift 745',
    '	.	error',
    '',
    '	cast_target	goto 1057',
    '	qualified_name	goto 301',
    '	approximate_numeric_type	goto 718',
    '	exact_numeric_type	goto 719',
    '	interval_type	goto 720',
    '	datetime_type	goto 721',
    '	numeric_type	goto 722',
    '	bit_string_type	goto 723',
    '	national_character_string_type	goto 724',
    '	character_string_type	goto 725',
    '	domain_name	goto 1058',
    '	data_type	goto 1059',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 866:',
    '',
    '	case_abbreviation : _COALESCE left_paren expression_list right_paren _	(444)',
    '',
    '	.	reduce 444',
    '',
    'state 867:',
    '',
    '	expression_list : expression_list comma _ expression',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 1060',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 868:',
    '',
    '	form_of_use_conversion : _CONVERT left_paren expression _USING _ form_of_use_conversion_name right_paren',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	form_of_use_conversion_name	goto 1061',
    '	qualified_name	goto 1062',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 869:',
    '',
    '	current_time_value_function : _CURRENT_TIME left_paren time_precision right_paren _	(224)',
    '',
    '	.	reduce 224',
    '',
    'state 870:',
    '',
    '	current_timestamp_value_function : _CURRENT_TIMESTAMP left_paren timestamp_precision right_paren _	(226)',
    '',
    '	.	reduce 226',
    '',
    'state 871:',
    '',
    '	extract_expression : _EXTRACT left_paren extract_field _FROM _ extract_source right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	extract_source	goto 1063',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 1064',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 872:',
    '',
    '	fold : _LOWER left_paren expression right_paren _	(478)',
    '',
    '	.	reduce 478',
    '',
    'state 873:',
    '',
    '	case_abbreviation : _NULLIF left_paren expression comma _ expression right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 1065',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 874:',
    '',
    '	octet_length_expression : _OCTET_LENGTH left_paren expression right_paren _	(510)',
    '',
    '	.	reduce 510',
    '',
    'state 875:',
    '',
    '	position_expression : _POSITION left_paren expression _IN _ expression right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 1066',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 876:',
    '',
    '	character_bit_substring_function : _SUBSTRING left_paren expression _FROM _ start_position for_strlength_opt right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	start_position	goto 1067',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 1068',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 877:',
    '',
    '	character_translation : _TRANSLATE left_paren expression _USING _ translation_name right_paren',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	translation_name	goto 1069',
    '	qualified_name	goto 322',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 878:',
    '',
    '	trim_operands : trim_character _FROM _ trim_source',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_source	goto 1070',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 1071',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 879:',
    '',
    '	trim_operands : trim_specification trim_character _ _FROM trim_source',
    '',
    '	_FROM	shift 1072',
    '	.	error',
    '',
    'state 880:',
    '',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	trim_character : expression _	(491)',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	_FROM	reduce 491',
    '	.	error',
    '',
    'state 881:',
    '',
    '	trim_operands : trim_specification _FROM _ trim_source',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_source	goto 1073',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 1071',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 882:',
    '',
    '	trim_function : _TRIM left_paren trim_operands right_paren _	(483)',
    '',
    '	.	reduce 483',
    '',
    'state 883:',
    '',
    '	fold : _UPPER left_paren expression right_paren _	(477)',
    '',
    '	.	reduce 477',
    '',
    'state 884:',
    '',
    '	corresponding_column_list_opt : _BY left_paren _ corresponding_column_list right_paren',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	corresponding_column_list	goto 1074',
    '	column_name_list	goto 1075',
    '	column_name	goto 551',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 885:',
    '',
    '	sort_specification : sort_key collate_clause_opt ordering_specification_opt _	(591)',
    '',
    '	.	reduce 591',
    '',
    'state 886:',
    '',
    '	ordering_specification_opt : _ASC _	(595)',
    '',
    '	.	reduce 595',
    '',
    'state 887:',
    '',
    '	ordering_specification_opt : _DESC _	(596)',
    '',
    '	.	reduce 596',
    '',
    'state 888:',
    '',
    '	sort_specification_list : sort_specification_list comma sort_specification _	(590)',
    '',
    '	.	reduce 590',
    '',
    'state 889:',
    '',
    '	declare_cursor : _DECLARE cursor_name insensitive_opt _ scroll_opt _CURSOR _FOR cursor_specification',
    '	dynamic_declare_cursor : _DECLARE cursor_name insensitive_opt _ scroll_opt _CURSOR _FOR statement_name',
    '	scroll_opt : _	(583)',
    '',
    '	_SCROLL	shift 1077',
    '	_CURSOR	reduce 583',
    '	.	error',
    '',
    '	scroll_opt	goto 1076',
    '',
    'state 890:',
    '',
    '	insensitive_opt : _INSENSITIVE _	(582)',
    '',
    '	.	reduce 582',
    '',
    'state 891:',
    '',
    '	procedure : _PROCEDURE procedure_name parameter_declaration_list _ semicolon SQL_procedure_statement semicolon',
    '',
    '	semicolon	shift 1078',
    '	.	error',
    '',
    'state 892:',
    '',
    '	parameter_declaration_list : left_paren _ parameter_declarations right_paren',
    '',
    '	colon	shift 151',
    '	_SQLCODE	shift 1083',
    '	_SQLSTATE	shift 1084',
    '	.	error',
    '',
    '	status_parameter	goto 1079',
    '	parameter_declaration	goto 1080',
    '	parameter_declarations	goto 1081',
    '	parameter_name	goto 1082',
    '',
    'state 893:',
    '',
    '	module_authorization_clause : _SCHEMA schema_name _AUTHORIZATION module_authorization_identifier _	(77)',
    '',
    '	.	reduce 77',
    '',
    'state 894:',
    '',
    '	character_set_name : identifier period identifier period SQL_language_identifier _	(33)',
    '',
    '	.	reduce 33',
    '',
    'state 895:',
    '',
    '	SQL_language_identifier : regular_identifier _	(42)',
    '',
    '	.	reduce 42',
    '',
    'state 896:',
    '',
    '	domain_constraint : constraint_name_definition_opt check_constraint_definition _ constraint_attributes_opt',
    '	constraint_attributes_opt : _	(558)',
    '',
    '	_DEFERRABLE	shift 713',
    '	_INITIALLY	shift 714',
    '	$end	reduce 558',
    '	identifier_body	reduce 558',
    '	delimited_identifier	reduce 558',
    '	left_paren	reduce 558',
    '	semicolon	reduce 558',
    '	underscore	reduce 558',
    '	_ALTER	reduce 558',
    '	_COLLATE	reduce 558',
    '	_COMMIT	reduce 558',
    '	_CONNECT	reduce 558',
    '	_CREATE	reduce 558',
    '	_DECLARE	reduce 558',
    '	_DELETE	reduce 558',
    '	_DISCONNECT	reduce 558',
    '	_DROP	reduce 558',
    '	_GRANT	reduce 558',
    '	_INSERT	reduce 558',
    '	_REVOKE	reduce 558',
    '	_ROLLBACK	reduce 558',
    '	_SELECT	reduce 558',
    '	_SET	reduce 558',
    '	_TABLE	reduce 558',
    '	_UPDATE	reduce 558',
    '	_VALUES	reduce 558',
    '	.	error',
    '',
    '	constraint_check_time	goto 710',
    '	constraint_attributes	goto 711',
    '	constraint_attributes_opt	goto 1085',
    '',
    'state 897:',
    '',
    '	check_constraint_definition : _CHECK _ left_paren search_condition right_paren',
    '',
    '	left_paren	shift 1086',
    '	.	error',
    '',
    'state 898:',
    '',
    '	constraint_name_definition : _CONSTRAINT constraint_name _	(228)',
    '',
    '	.	reduce 228',
    '',
    'state 899:',
    '',
    '	drop_domain_constraint_definition : _DROP _CONSTRAINT constraint_name _	(772)',
    '',
    '	.	reduce 772',
    '',
    'state 900:',
    '',
    '	default_option : datetime_value_function _	(195)',
    '',
    '	.	reduce 195',
    '',
    'state 901:',
    '',
    '	default_option : literal _	(194)',
    '',
    '	.	reduce 194',
    '',
    'state 902:',
    '',
    '	default_clause : _DEFAULT default_option _	(193)',
    '',
    '	.	reduce 193',
    '',
    'state 903:',
    '',
    '	default_option : _CURRENT_USER _	(197)',
    '',
    '	.	reduce 197',
    '',
    'state 904:',
    '',
    '	default_option : _NULL _	(200)',
    '',
    '	.	reduce 200',
    '',
    'state 905:',
    '',
    '	default_option : _SESSION_USER _	(198)',
    '',
    '	.	reduce 198',
    '',
    'state 906:',
    '',
    '	default_option : _SYSTEM_USER _	(199)',
    '',
    '	.	reduce 199',
    '',
    'state 907:',
    '',
    '	default_option : _USER _	(196)',
    '',
    '	.	reduce 196',
    '',
    'state 908:',
    '',
    '	column_definition : column_name _ column_definition_sel default_clause_opt column_constraint_definition_opt collate_clause_opt',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_BIT	shift 727',
    '	_CHAR	shift 728',
    '	_CHARACTER	shift 729',
    '	_DATE	shift 730',
    '	_DEC	shift 731',
    '	_DECIMAL	shift 732',
    '	_DOUBLE	shift 733',
    '	_FLOAT	shift 734',
    '	_INT	shift 735',
    '	_INTEGER	shift 736',
    '	_INTERVAL	shift 737',
    '	_NATIONAL	shift 738',
    '	_NCHAR	shift 739',
    '	_NUMERIC	shift 740',
    '	_REAL	shift 741',
    '	_SMALLINT	shift 742',
    '	_TIME	shift 743',
    '	_TIMESTAMP	shift 744',
    '	_VARCHAR	shift 745',
    '	.	error',
    '',
    '	qualified_name	goto 301',
    '	approximate_numeric_type	goto 718',
    '	exact_numeric_type	goto 719',
    '	interval_type	goto 720',
    '	datetime_type	goto 721',
    '	numeric_type	goto 722',
    '	bit_string_type	goto 723',
    '	national_character_string_type	goto 724',
    '	character_string_type	goto 725',
    '	domain_name	goto 1087',
    '	data_type	goto 1088',
    '	column_definition_sel	goto 1089',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 909:',
    '',
    '	add_column_definition : _ADD column_opt column_definition _	(750)',
    '',
    '	.	reduce 750',
    '',
    'state 910:',
    '',
    '	table_constraint : referential_constraint_definition _	(571)',
    '',
    '	.	reduce 571',
    '',
    'state 911:',
    '',
    '	table_constraint : unique_constraint_definition _	(570)',
    '',
    '	.	reduce 570',
    '',
    'state 912:',
    '',
    '	table_constraint_definition : constraint_name_definition_opt table_constraint _ constraint_check_time_opt',
    '	constraint_check_time_opt : _	(565)',
    '',
    '	_INITIALLY	shift 714',
    '	$end	reduce 565',
    '	identifier_body	reduce 565',
    '	delimited_identifier	reduce 565',
    '	left_paren	reduce 565',
    '	right_paren	reduce 565',
    '	comma	reduce 565',
    '	semicolon	reduce 565',
    '	underscore	reduce 565',
    '	_ALTER	reduce 565',
    '	_COMMIT	reduce 565',
    '	_CONNECT	reduce 565',
    '	_CREATE	reduce 565',
    '	_DECLARE	reduce 565',
    '	_DELETE	reduce 565',
    '	_DISCONNECT	reduce 565',
    '	_DROP	reduce 565',
    '	_GRANT	reduce 565',
    '	_INSERT	reduce 565',
    '	_REVOKE	reduce 565',
    '	_ROLLBACK	reduce 565',
    '	_SELECT	reduce 565',
    '	_SET	reduce 565',
    '	_TABLE	reduce 565',
    '	_UPDATE	reduce 565',
    '	_VALUES	reduce 565',
    '	.	error',
    '',
    '	constraint_check_time_opt	goto 1090',
    '	constraint_check_time	goto 932',
    '',
    'state 913:',
    '',
    '	table_constraint : check_constraint_definition _	(572)',
    '',
    '	.	reduce 572',
    '',
    'state 914:',
    '',
    '	unique_constraint_definition : unique_specification _ left_paren unique_column_list right_paren',
    '',
    '	left_paren	shift 1091',
    '	.	error',
    '',
    'state 915:',
    '',
    '	referential_constraint_definition : _FOREIGN _ _KEY left_paren referencing_columns right_paren references_specification',
    '',
    '	_KEY	shift 1092',
    '	.	error',
    '',
    'state 916:',
    '',
    '	unique_specification : _PRIMARY _ _KEY',
    '',
    '	_KEY	shift 1093',
    '	.	error',
    '',
    'state 917:',
    '',
    '	unique_specification : _UNIQUE _	(236)',
    '',
    '	.	reduce 236',
    '',
    'state 918:',
    '',
    '	alter_column_definition : _ALTER column_opt column_name _ alter_column_action',
    '',
    '	_DROP	shift 1097',
    '	_SET	shift 1098',
    '	.	error',
    '',
    '	drop_column_default_clause	goto 1094',
    '	set_column_default_clause	goto 1095',
    '	alter_column_action	goto 1096',
    '',
    'state 919:',
    '',
    '	drop_column_definition : _DROP column_opt column_name _ drop_behaviour',
    '',
    '	_CASCADE	shift 536',
    '	_RESTRICT	shift 537',
    '	.	error',
    '',
    '	drop_behaviour	goto 1099',
    '',
    'state 920:',
    '',
    '	drop_table_constraint_definition : _DROP _CONSTRAINT constraint_name _ drop_behaviour',
    '',
    '	_CASCADE	shift 536',
    '	_RESTRICT	shift 537',
    '	.	error',
    '',
    '	drop_behaviour	goto 1100',
    '',
    'state 921:',
    '',
    '	user_name_opt : _USER user_name _	(861)',
    '',
    '	.	reduce 861',
    '',
    'state 922:',
    '',
    '	user_name : simple_value_specification _	(864)',
    '',
    '	.	reduce 864',
    '',
    'state 923:',
    '',
    '	table_definition : _CREATE table_definition_opts _TABLE table_name table_element_list table_commit_opts _	(651)',
    '',
    '	.	reduce 651',
    '',
    'state 924:',
    '',
    '	table_commit_opts : _ON _ _COMMIT _DELETE _ROWS',
    '	table_commit_opts : _ON _ _COMMIT _PRESERVE _ROWS',
    '',
    '	_COMMIT	shift 1101',
    '	.	error',
    '',
    'state 925:',
    '',
    '	table_element : table_constraint_definition _	(90)',
    '',
    '	.	reduce 90',
    '',
    'state 926:',
    '',
    '	table_element : column_definition _	(89)',
    '',
    '	.	reduce 89',
    '',
    'state 927:',
    '',
    '	table_element_list : left_paren table_element _ table_element_list_opt right_paren',
    '	table_element_list_opt : _	(87)',
    '',
    '	.	reduce 87',
    '',
    '	table_element_list_opt	goto 1102',
    '',
    'state 928:',
    '',
    '	constraint_attributes : constraint_check_time deferrable_opt _	(560)',
    '',
    '	.	reduce 560',
    '',
    'state 929:',
    '',
    '	deferrable_opt : _DEFERRABLE _	(563)',
    '',
    '	.	reduce 563',
    '',
    'state 930:',
    '',
    '	deferrable_opt : _NOT _ _DEFERRABLE',
    '',
    '	_DEFERRABLE	shift 1103',
    '	.	error',
    '',
    'state 931:',
    '',
    '	constraint_attributes : _DEFERRABLE constraint_check_time_opt _	(561)',
    '',
    '	.	reduce 561',
    '',
    'state 932:',
    '',
    '	constraint_check_time_opt : constraint_check_time _	(566)',
    '',
    '	.	reduce 566',
    '',
    'state 933:',
    '',
    '	constraint_check_time : _INITIALLY _DEFERRED _	(567)',
    '',
    '	.	reduce 567',
    '',
    'state 934:',
    '',
    '	constraint_check_time : _INITIALLY _IMMEDIATE _	(568)',
    '',
    '	.	reduce 568',
    '',
    'state 935:',
    '',
    '	assertion_check : _CHECK left_paren search_condition _ right_paren',
    '	search_condition : search_condition _ _OR boolean_term',
    '',
    '	right_paren	shift 1104',
    '	_OR	shift 857',
    '	.	error',
    '',
    'state 936:',
    '',
    '	character_set_definition : _CREATE _CHARACTER _SET character_set_name as_opt character_set_source _ charset_collation_opt',
    '	charset_collation_opt : _	(696)',
    '',
    '	_COLLATE	shift 414',
    '	_COLLATION	shift 1108',
    '	$end	reduce 696',
    '	identifier_body	reduce 696',
    '	delimited_identifier	reduce 696',
    '	left_paren	reduce 696',
    '	semicolon	reduce 696',
    '	underscore	reduce 696',
    '	_ALTER	reduce 696',
    '	_COMMIT	reduce 696',
    '	_CONNECT	reduce 696',
    '	_CREATE	reduce 696',
    '	_DECLARE	reduce 696',
    '	_DELETE	reduce 696',
    '	_DISCONNECT	reduce 696',
    '	_DROP	reduce 696',
    '	_GRANT	reduce 696',
    '	_INSERT	reduce 696',
    '	_REVOKE	reduce 696',
    '	_ROLLBACK	reduce 696',
    '	_SELECT	reduce 696',
    '	_SET	reduce 696',
    '	_TABLE	reduce 696',
    '	_UPDATE	reduce 696',
    '	_VALUES	reduce 696',
    '	.	error',
    '',
    '	limited_collation_definition	goto 1105',
    '	charset_collation_opt	goto 1106',
    '	collate_clause	goto 1107',
    '',
    'state 937:',
    '',
    '	character_set_source : _GET _ existing_character_set_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	existing_character_set_name	goto 1109',
    '	actual_identifier	goto 61',
    '	SQL_language_identifier	goto 96',
    '	identifier	goto 97',
    '	character_set_name	goto 1110',
    '	introducer	goto 63',
    '	regular_identifier	goto 100',
    '',
    'state 938:',
    '',
    '	collation_definition : _CREATE _COLLATION collation_name _FOR character_set_specification _FROM _ collation_source pad_attribute_opt',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_DEFAULT	shift 1117',
    '	_DESC	shift 1118',
    '	_EXTERNAL	shift 1119',
    '	_TRANSLATION	shift 1120',
    '	.	error',
    '',
    '	schema_collation_name	goto 1111',
    '	external_collation	goto 1112',
    '	translation_collation	goto 1113',
    '	collating_sequence_definition	goto 1114',
    '	collation_source	goto 1115',
    '	collation_name	goto 1116',
    '	qualified_name	goto 313',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 939:',
    '',
    '	data_type : character_string_type data_type_opt _	(101)',
    '',
    '	.	reduce 101',
    '',
    'state 940:',
    '',
    '	data_type_opt : _CHARACTER _ _SET character_set_specification',
    '',
    '	_SET	shift 1121',
    '	.	error',
    '',
    'state 941:',
    '',
    '	default_clause_opt : default_clause _	(95)',
    '',
    '	.	reduce 95',
    '',
    'state 942:',
    '',
    '	domain_definition : _CREATE _DOMAIN domain_name as_opt data_type default_clause_opt _ domain_constraint_opt collate_clause_opt',
    '	constraint_name_definition_opt : _	(229)',
    '	domain_constraint_opt : _	(648)',
    '',
    '	_CONSTRAINT	shift 693',
    '	_CHECK	reduce 229',
    '	$end	reduce 648',
    '	identifier_body	reduce 648',
    '	delimited_identifier	reduce 648',
    '	left_paren	reduce 648',
    '	semicolon	reduce 648',
    '	underscore	reduce 648',
    '	_ALTER	reduce 648',
    '	_COLLATE	reduce 648',
    '	_COMMIT	reduce 648',
    '	_CONNECT	reduce 648',
    '	_CREATE	reduce 648',
    '	_DECLARE	reduce 648',
    '	_DELETE	reduce 648',
    '	_DISCONNECT	reduce 648',
    '	_DROP	reduce 648',
    '	_GRANT	reduce 648',
    '	_INSERT	reduce 648',
    '	_REVOKE	reduce 648',
    '	_ROLLBACK	reduce 648',
    '	_SELECT	reduce 648',
    '	_SET	reduce 648',
    '	_TABLE	reduce 648',
    '	_UPDATE	reduce 648',
    '	_VALUES	reduce 648',
    '	.	error',
    '',
    '	domain_constraint	goto 1122',
    '	domain_constraint_opt	goto 1123',
    '	constraint_name_definition	goto 691',
    '	constraint_name_definition_opt	goto 692',
    '',
    'state 943:',
    '',
    '	bit_string_type : _BIT character_string_type_len _	(133)',
    '',
    '	.	reduce 133',
    '',
    'state 944:',
    '',
    '	character_string_type_len : left_paren _ length right_paren',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	length	goto 1124',
    '	unsigned_integer	goto 1125',
    '',
    'state 945:',
    '',
    '	*** conflicts:',
    '',
    '	shift 944, reduce 136 on left_paren',
    '',
    '	bit_string_type : _BIT _VARYING _ character_string_type_len',
    '	bit_string_type : _BIT _VARYING _	(136)',
    '',
    '	left_paren	shift 944',
    '	$end	reduce 136',
    '	identifier_body	reduce 136',
    '	delimited_identifier	reduce 136',
    '	right_paren	reduce 136',
    '	comma	reduce 136',
    '	semicolon	reduce 136',
    '	underscore	reduce 136',
    '	_ALTER	reduce 136',
    '	_CHECK	reduce 136',
    '	_COLLATE	reduce 136',
    '	_COMMIT	reduce 136',
    '	_CONNECT	reduce 136',
    '	_CONSTRAINT	reduce 136',
    '	_CREATE	reduce 136',
    '	_DECLARE	reduce 136',
    '	_DEFAULT	reduce 136',
    '	_DELETE	reduce 136',
    '	_DISCONNECT	reduce 136',
    '	_DROP	reduce 136',
    '	_GRANT	reduce 136',
    '	_INSERT	reduce 136',
    '	_NOT	reduce 136',
    '	_PRIMARY	reduce 136',
    '	_REFERENCES	reduce 136',
    '	_REVOKE	reduce 136',
    '	_ROLLBACK	reduce 136',
    '	_SELECT	reduce 136',
    '	_SET	reduce 136',
    '	_TABLE	reduce 136',
    '	_UNIQUE	reduce 136',
    '	_UPDATE	reduce 136',
    '	_VALUES	reduce 136',
    '	.	error',
    '',
    '	character_string_type_len	goto 1126',
    '',
    'state 946:',
    '',
    '	character_string_type : _CHAR character_string_type_len _	(110)',
    '',
    '	.	reduce 110',
    '',
    'state 947:',
    '',
    '	*** conflicts:',
    '',
    '	shift 944, reduce 117 on left_paren',
    '',
    '	character_string_type : _CHAR _VARYING _ character_string_type_len',
    '	character_string_type : _CHAR _VARYING _	(117)',
    '',
    '	left_paren	shift 944',
    '	$end	reduce 117',
    '	identifier_body	reduce 117',
    '	delimited_identifier	reduce 117',
    '	right_paren	reduce 117',
    '	comma	reduce 117',
    '	semicolon	reduce 117',
    '	underscore	reduce 117',
    '	_ALTER	reduce 117',
    '	_CHARACTER	reduce 117',
    '	_CHECK	reduce 117',
    '	_COLLATE	reduce 117',
    '	_COMMIT	reduce 117',
    '	_CONNECT	reduce 117',
    '	_CONSTRAINT	reduce 117',
    '	_CREATE	reduce 117',
    '	_DECLARE	reduce 117',
    '	_DEFAULT	reduce 117',
    '	_DELETE	reduce 117',
    '	_DISCONNECT	reduce 117',
    '	_DROP	reduce 117',
    '	_GRANT	reduce 117',
    '	_INSERT	reduce 117',
    '	_NOT	reduce 117',
    '	_PRIMARY	reduce 117',
    '	_REFERENCES	reduce 117',
    '	_REVOKE	reduce 117',
    '	_ROLLBACK	reduce 117',
    '	_SELECT	reduce 117',
    '	_SET	reduce 117',
    '	_TABLE	reduce 117',
    '	_UNIQUE	reduce 117',
    '	_UPDATE	reduce 117',
    '	_VALUES	reduce 117',
    '	.	error',
    '',
    '	character_string_type_len	goto 1127',
    '',
    'state 948:',
    '',
    '	character_string_type : _CHARACTER character_string_type_len _	(109)',
    '',
    '	.	reduce 109',
    '',
    'state 949:',
    '',
    '	*** conflicts:',
    '',
    '	shift 944, reduce 116 on left_paren',
    '',
    '	character_string_type : _CHARACTER _VARYING _ character_string_type_len',
    '	character_string_type : _CHARACTER _VARYING _	(116)',
    '',
    '	left_paren	shift 944',
    '	$end	reduce 116',
    '	identifier_body	reduce 116',
    '	delimited_identifier	reduce 116',
    '	right_paren	reduce 116',
    '	comma	reduce 116',
    '	semicolon	reduce 116',
    '	underscore	reduce 116',
    '	_ALTER	reduce 116',
    '	_CHARACTER	reduce 116',
    '	_CHECK	reduce 116',
    '	_COLLATE	reduce 116',
    '	_COMMIT	reduce 116',
    '	_CONNECT	reduce 116',
    '	_CONSTRAINT	reduce 116',
    '	_CREATE	reduce 116',
    '	_DECLARE	reduce 116',
    '	_DEFAULT	reduce 116',
    '	_DELETE	reduce 116',
    '	_DISCONNECT	reduce 116',
    '	_DROP	reduce 116',
    '	_GRANT	reduce 116',
    '	_INSERT	reduce 116',
    '	_NOT	reduce 116',
    '	_PRIMARY	reduce 116',
    '	_REFERENCES	reduce 116',
    '	_REVOKE	reduce 116',
    '	_ROLLBACK	reduce 116',
    '	_SELECT	reduce 116',
    '	_SET	reduce 116',
    '	_TABLE	reduce 116',
    '	_UNIQUE	reduce 116',
    '	_UPDATE	reduce 116',
    '	_VALUES	reduce 116',
    '	.	error',
    '',
    '	character_string_type_len	goto 1128',
    '',
    'state 950:',
    '',
    '	exact_numeric_type : _DEC numeric_precision_scale_opt _	(141)',
    '',
    '	.	reduce 141',
    '',
    'state 951:',
    '',
    '	numeric_precision_scale_opt : left_paren _ precision comma scale right_paren',
    '	numeric_precision_scale_opt : left_paren _ precision right_paren',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	precision	goto 1129',
    '	unsigned_integer	goto 825',
    '',
    'state 952:',
    '',
    '	exact_numeric_type : _DECIMAL numeric_precision_scale_opt _	(140)',
    '',
    '	.	reduce 140',
    '',
    'state 953:',
    '',
    '	approximate_numeric_type : _DOUBLE _PRECISION _	(153)',
    '',
    '	.	reduce 153',
    '',
    'state 954:',
    '',
    '	approximate_numeric_type : _FLOAT left_paren _ precision right_paren',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	precision	goto 1130',
    '	unsigned_integer	goto 825',
    '',
    'state 955:',
    '',
    '	interval_type : _INTERVAL interval_qualifier _	(166)',
    '',
    '	.	reduce 166',
    '',
    'state 956:',
    '',
    '	*** conflicts:',
    '',
    '	shift 944, reduce 128 on left_paren',
    '',
    '	national_character_string_type : _NATIONAL _CHAR _ character_string_type_len',
    '	national_character_string_type : _NATIONAL _CHAR _ _VARYING character_string_type_len',
    '	national_character_string_type : _NATIONAL _CHAR _	(128)',
    '	national_character_string_type : _NATIONAL _CHAR _ _VARYING',
    '',
    '	left_paren	shift 944',
    '	_VARYING	shift 1132',
    '	$end	reduce 128',
    '	identifier_body	reduce 128',
    '	delimited_identifier	reduce 128',
    '	right_paren	reduce 128',
    '	comma	reduce 128',
    '	semicolon	reduce 128',
    '	underscore	reduce 128',
    '	_ALTER	reduce 128',
    '	_CHECK	reduce 128',
    '	_COLLATE	reduce 128',
    '	_COMMIT	reduce 128',
    '	_CONNECT	reduce 128',
    '	_CONSTRAINT	reduce 128',
    '	_CREATE	reduce 128',
    '	_DECLARE	reduce 128',
    '	_DEFAULT	reduce 128',
    '	_DELETE	reduce 128',
    '	_DISCONNECT	reduce 128',
    '	_DROP	reduce 128',
    '	_GRANT	reduce 128',
    '	_INSERT	reduce 128',
    '	_NOT	reduce 128',
    '	_PRIMARY	reduce 128',
    '	_REFERENCES	reduce 128',
    '	_REVOKE	reduce 128',
    '	_ROLLBACK	reduce 128',
    '	_SELECT	reduce 128',
    '	_SET	reduce 128',
    '	_TABLE	reduce 128',
    '	_UNIQUE	reduce 128',
    '	_UPDATE	reduce 128',
    '	_VALUES	reduce 128',
    '	.	error',
    '',
    '	character_string_type_len	goto 1131',
    '',
    'state 957:',
    '',
    '	*** conflicts:',
    '',
    '	shift 944, reduce 127 on left_paren',
    '',
    '	national_character_string_type : _NATIONAL _CHARACTER _ character_string_type_len',
    '	national_character_string_type : _NATIONAL _CHARACTER _ _VARYING character_string_type_len',
    '	national_character_string_type : _NATIONAL _CHARACTER _	(127)',
    '	national_character_string_type : _NATIONAL _CHARACTER _ _VARYING',
    '',
    '	left_paren	shift 944',
    '	_VARYING	shift 1134',
    '	$end	reduce 127',
    '	identifier_body	reduce 127',
    '	delimited_identifier	reduce 127',
    '	right_paren	reduce 127',
    '	comma	reduce 127',
    '	semicolon	reduce 127',
    '	underscore	reduce 127',
    '	_ALTER	reduce 127',
    '	_CHECK	reduce 127',
    '	_COLLATE	reduce 127',
    '	_COMMIT	reduce 127',
    '	_CONNECT	reduce 127',
    '	_CONSTRAINT	reduce 127',
    '	_CREATE	reduce 127',
    '	_DECLARE	reduce 127',
    '	_DEFAULT	reduce 127',
    '	_DELETE	reduce 127',
    '	_DISCONNECT	reduce 127',
    '	_DROP	reduce 127',
    '	_GRANT	reduce 127',
    '	_INSERT	reduce 127',
    '	_NOT	reduce 127',
    '	_PRIMARY	reduce 127',
    '	_REFERENCES	reduce 127',
    '	_REVOKE	reduce 127',
    '	_ROLLBACK	reduce 127',
    '	_SELECT	reduce 127',
    '	_SET	reduce 127',
    '	_TABLE	reduce 127',
    '	_UNIQUE	reduce 127',
    '	_UPDATE	reduce 127',
    '	_VALUES	reduce 127',
    '	.	error',
    '',
    '	character_string_type_len	goto 1133',
    '',
    'state 958:',
    '',
    '	national_character_string_type : _NCHAR character_string_type_len _	(123)',
    '',
    '	.	reduce 123',
    '',
    'state 959:',
    '',
    '	*** conflicts:',
    '',
    '	shift 944, reduce 132 on left_paren',
    '',
    '	national_character_string_type : _NCHAR _VARYING _ character_string_type_len',
    '	national_character_string_type : _NCHAR _VARYING _	(132)',
    '',
    '	left_paren	shift 944',
    '	$end	reduce 132',
    '	identifier_body	reduce 132',
    '	delimited_identifier	reduce 132',
    '	right_paren	reduce 132',
    '	comma	reduce 132',
    '	semicolon	reduce 132',
    '	underscore	reduce 132',
    '	_ALTER	reduce 132',
    '	_CHECK	reduce 132',
    '	_COLLATE	reduce 132',
    '	_COMMIT	reduce 132',
    '	_CONNECT	reduce 132',
    '	_CONSTRAINT	reduce 132',
    '	_CREATE	reduce 132',
    '	_DECLARE	reduce 132',
    '	_DEFAULT	reduce 132',
    '	_DELETE	reduce 132',
    '	_DISCONNECT	reduce 132',
    '	_DROP	reduce 132',
    '	_GRANT	reduce 132',
    '	_INSERT	reduce 132',
    '	_NOT	reduce 132',
    '	_PRIMARY	reduce 132',
    '	_REFERENCES	reduce 132',
    '	_REVOKE	reduce 132',
    '	_ROLLBACK	reduce 132',
    '	_SELECT	reduce 132',
    '	_SET	reduce 132',
    '	_TABLE	reduce 132',
    '	_UNIQUE	reduce 132',
    '	_UPDATE	reduce 132',
    '	_VALUES	reduce 132',
    '	.	error',
    '',
    '	character_string_type_len	goto 1135',
    '',
    'state 960:',
    '',
    '	exact_numeric_type : _NUMERIC numeric_precision_scale_opt _	(139)',
    '',
    '	.	reduce 139',
    '',
    'state 961:',
    '',
    '	datetime_type : _TIME time_precision_opt _ tz_opt',
    '	tz_opt : _	(161)',
    '',
    '	_WITH	shift 1137',
    '	$end	reduce 161',
    '	identifier_body	reduce 161',
    '	delimited_identifier	reduce 161',
    '	left_paren	reduce 161',
    '	right_paren	reduce 161',
    '	comma	reduce 161',
    '	semicolon	reduce 161',
    '	underscore	reduce 161',
    '	_ALTER	reduce 161',
    '	_CHECK	reduce 161',
    '	_COLLATE	reduce 161',
    '	_COMMIT	reduce 161',
    '	_CONNECT	reduce 161',
    '	_CONSTRAINT	reduce 161',
    '	_CREATE	reduce 161',
    '	_DECLARE	reduce 161',
    '	_DEFAULT	reduce 161',
    '	_DELETE	reduce 161',
    '	_DISCONNECT	reduce 161',
    '	_DROP	reduce 161',
    '	_GRANT	reduce 161',
    '	_INSERT	reduce 161',
    '	_NOT	reduce 161',
    '	_PRIMARY	reduce 161',
    '	_REFERENCES	reduce 161',
    '	_REVOKE	reduce 161',
    '	_ROLLBACK	reduce 161',
    '	_SELECT	reduce 161',
    '	_SET	reduce 161',
    '	_TABLE	reduce 161',
    '	_UNIQUE	reduce 161',
    '	_UPDATE	reduce 161',
    '	_VALUES	reduce 161',
    '	.	error',
    '',
    '	tz_opt	goto 1136',
    '',
    'state 962:',
    '',
    '	time_precision_opt : left_paren _ time_precision right_paren',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	time_fractional_seconds_precision	goto 645',
    '	time_precision	goto 1138',
    '	unsigned_integer	goto 647',
    '',
    'state 963:',
    '',
    '	datetime_type : _TIMESTAMP timestamp_precision_opt _ tz_opt',
    '	tz_opt : _	(161)',
    '',
    '	_WITH	shift 1137',
    '	$end	reduce 161',
    '	identifier_body	reduce 161',
    '	delimited_identifier	reduce 161',
    '	left_paren	reduce 161',
    '	right_paren	reduce 161',
    '	comma	reduce 161',
    '	semicolon	reduce 161',
    '	underscore	reduce 161',
    '	_ALTER	reduce 161',
    '	_CHECK	reduce 161',
    '	_COLLATE	reduce 161',
    '	_COMMIT	reduce 161',
    '	_CONNECT	reduce 161',
    '	_CONSTRAINT	reduce 161',
    '	_CREATE	reduce 161',
    '	_DECLARE	reduce 161',
    '	_DEFAULT	reduce 161',
    '	_DELETE	reduce 161',
    '	_DISCONNECT	reduce 161',
    '	_DROP	reduce 161',
    '	_GRANT	reduce 161',
    '	_INSERT	reduce 161',
    '	_NOT	reduce 161',
    '	_PRIMARY	reduce 161',
    '	_REFERENCES	reduce 161',
    '	_REVOKE	reduce 161',
    '	_ROLLBACK	reduce 161',
    '	_SELECT	reduce 161',
    '	_SET	reduce 161',
    '	_TABLE	reduce 161',
    '	_UNIQUE	reduce 161',
    '	_UPDATE	reduce 161',
    '	_VALUES	reduce 161',
    '	.	error',
    '',
    '	tz_opt	goto 1139',
    '',
    'state 964:',
    '',
    '	timestamp_precision_opt : left_paren _ timestamp_precision right_paren',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	time_fractional_seconds_precision	goto 648',
    '	timestamp_precision	goto 1140',
    '	unsigned_integer	goto 647',
    '',
    'state 965:',
    '',
    '	character_string_type : _VARCHAR character_string_type_len _	(113)',
    '',
    '	.	reduce 113',
    '',
    'state 966:',
    '',
    '	schema_elements : schema_elements schema_element _	(633)',
    '',
    '	.	reduce 633',
    '',
    'state 967:',
    '',
    '	schema_character_set_specification : _DEFAULT _CHARACTER _SET _ character_set_specification',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	actual_identifier	goto 61',
    '	SQL_language_identifier	goto 96',
    '	identifier	goto 97',
    '	character_set_name	goto 98',
    '	character_set_specification	goto 1141',
    '	introducer	goto 63',
    '	regular_identifier	goto 100',
    '',
    'state 968:',
    '',
    '	translation_definition : _CREATE _TRANSLATION translation_name _FOR source_character_set_specification _TO _ target_character_set_specification _FROM translation_source',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	target_character_set_specification	goto 1142',
    '	actual_identifier	goto 61',
    '	SQL_language_identifier	goto 96',
    '	identifier	goto 97',
    '	character_set_name	goto 98',
    '	character_set_specification	goto 1143',
    '	introducer	goto 63',
    '	regular_identifier	goto 100',
    '',
    'state 969:',
    '',
    '	view_definition : _CREATE _VIEW table_name view_column_list_opt _AS query_expression _ view_check_opt',
    '	query_expression : query_expression _ _UNION all_opt corresponding_spec_opt query_term',
    '	query_expression : query_expression _ _EXCEPT all_opt corresponding_spec_opt query_term',
    '	view_check_opt : _	(661)',
    '',
    '	_EXCEPT	shift 91',
    '	_UNION	shift 93',
    '	_WITH	shift 1145',
    '	$end	reduce 661',
    '	identifier_body	reduce 661',
    '	delimited_identifier	reduce 661',
    '	left_paren	reduce 661',
    '	semicolon	reduce 661',
    '	underscore	reduce 661',
    '	_ALTER	reduce 661',
    '	_COMMIT	reduce 661',
    '	_CONNECT	reduce 661',
    '	_CREATE	reduce 661',
    '	_DECLARE	reduce 661',
    '	_DELETE	reduce 661',
    '	_DISCONNECT	reduce 661',
    '	_DROP	reduce 661',
    '	_GRANT	reduce 661',
    '	_INSERT	reduce 661',
    '	_REVOKE	reduce 661',
    '	_ROLLBACK	reduce 661',
    '	_SELECT	reduce 661',
    '	_SET	reduce 661',
    '	_TABLE	reduce 661',
    '	_UPDATE	reduce 661',
    '	_VALUES	reduce 661',
    '	.	error',
    '',
    '	view_check_opt	goto 1144',
    '',
    'state 970:',
    '',
    '	view_column_list_opt : left_paren view_column_list right_paren _	(660)',
    '',
    '	.	reduce 660',
    '',
    'state 971:',
    '',
    '	temporary_table_declaration : _DECLARE _LOCAL _TEMPORARY _TABLE qualified_local_table_name table_element_list _ temporary_table_declaration_opt',
    '	temporary_table_declaration_opt : _	(81)',
    '',
    '	_ON	shift 1147',
    '	$end	reduce 81',
    '	identifier_body	reduce 81',
    '	delimited_identifier	reduce 81',
    '	left_paren	reduce 81',
    '	underscore	reduce 81',
    '	_ALTER	reduce 81',
    '	_COMMIT	reduce 81',
    '	_CONNECT	reduce 81',
    '	_CREATE	reduce 81',
    '	_DECLARE	reduce 81',
    '	_DELETE	reduce 81',
    '	_DISCONNECT	reduce 81',
    '	_DROP	reduce 81',
    '	_GRANT	reduce 81',
    '	_INSERT	reduce 81',
    '	_PROCEDURE	reduce 81',
    '	_REVOKE	reduce 81',
    '	_ROLLBACK	reduce 81',
    '	_SELECT	reduce 81',
    '	_SET	reduce 81',
    '	_TABLE	reduce 81',
    '	_UPDATE	reduce 81',
    '	_VALUES	reduce 81',
    '	.	error',
    '',
    '	temporary_table_declaration_opt	goto 1146',
    '',
    'state 972:',
    '',
    '	date_value : unsigned_integer minus_sign unsigned_integer _ minus_sign unsigned_integer',
    '	unsigned_integer : unsigned_integer _ digit',
    '',
    '	digit	shift 331',
    '	minus_sign	shift 1148',
    '	.	error',
    '',
    'state 973:',
    '',
    '	interval_string_literal : unsigned_integer space unsigned_integer _	(55)',
    '	interval_string_literal : unsigned_integer space unsigned_integer _ colon unsigned_integer',
    '	interval_string_literal : unsigned_integer space unsigned_integer _ colon unsigned_integer colon seconds_value',
    '	unsigned_integer : unsigned_integer _ digit',
    '',
    '	digit	shift 331',
    '	colon	shift 1149',
    '	quote	reduce 55',
    '	.	error',
    '',
    'state 974:',
    '',
    '	interval_string_literal : unsigned_integer minus_sign unsigned_integer _	(54)',
    '	unsigned_integer : unsigned_integer _ digit',
    '',
    '	digit	shift 331',
    '	quote	reduce 54',
    '	.	error',
    '',
    'state 975:',
    '',
    '	interval_string_literal : unsigned_integer period unsigned_integer _	(58)',
    '	unsigned_integer : unsigned_integer _ digit',
    '',
    '	digit	shift 331',
    '	quote	reduce 58',
    '	.	error',
    '',
    'state 976:',
    '',
    '	interval_string_literal : unsigned_integer colon seconds_value _	(59)',
    '',
    '	.	reduce 59',
    '',
    'state 977:',
    '',
    '	interval_string_literal : unsigned_integer colon unsigned_integer _ colon seconds_value',
    '	unsigned_integer : unsigned_integer _ digit',
    '	seconds_value : unsigned_integer _	(47)',
    '	seconds_value : unsigned_integer _ period unsigned_integer',
    '',
    '	digit	shift 331',
    '	period	shift 1150',
    '	colon	shift 1151',
    '	quote	reduce 47',
    '	.	error',
    '',
    'state 978:',
    '',
    '	time_string : quote time_value quote quote _ time_value time_zone_interval quote',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	time_value	goto 1152',
    '	unsigned_integer	goto 532',
    '',
    'state 979:',
    '',
    '	time_value : unsigned_integer colon unsigned_integer _ colon seconds_value',
    '	unsigned_integer : unsigned_integer _ digit',
    '',
    '	digit	shift 331',
    '	colon	shift 1153',
    '	.	error',
    '',
    'state 980:',
    '',
    '	timestamp_string : quote date_value space time_value _ quote',
    '	timestamp_string : quote date_value space time_value _ time_zone_interval quote',
    '',
    '	quote	shift 1156',
    '	plus_sign	shift 148',
    '	minus_sign	shift 149',
    '	.	error',
    '',
    '	time_zone_interval	goto 1154',
    '	sign	goto 1155',
    '',
    'state 981:',
    '',
    '	grantee_list : grantee _	(667)',
    '',
    '	.	reduce 667',
    '',
    'state 982:',
    '',
    '	grant_statement : _GRANT privileges _ON object_name _TO grantee_list _ grant_option',
    '	grantee_list : grantee_list _ comma grantee',
    '	grant_option : _	(669)',
    '',
    '	comma	shift 1158',
    '	_WITH	shift 1159',
    '	$end	reduce 669',
    '	identifier_body	reduce 669',
    '	delimited_identifier	reduce 669',
    '	left_paren	reduce 669',
    '	semicolon	reduce 669',
    '	underscore	reduce 669',
    '	_ALTER	reduce 669',
    '	_COMMIT	reduce 669',
    '	_CONNECT	reduce 669',
    '	_CREATE	reduce 669',
    '	_DECLARE	reduce 669',
    '	_DELETE	reduce 669',
    '	_DISCONNECT	reduce 669',
    '	_DROP	reduce 669',
    '	_GRANT	reduce 669',
    '	_INSERT	reduce 669',
    '	_REVOKE	reduce 669',
    '	_ROLLBACK	reduce 669',
    '	_SELECT	reduce 669',
    '	_SET	reduce 669',
    '	_TABLE	reduce 669',
    '	_UPDATE	reduce 669',
    '	_VALUES	reduce 669',
    '	.	error',
    '',
    '	grant_option	goto 1157',
    '',
    'state 983:',
    '',
    '	grantee : authorization_identifier _	(692)',
    '',
    '	.	reduce 692',
    '',
    'state 984:',
    '',
    '	grantee : _PUBLIC _	(691)',
    '',
    '	.	reduce 691',
    '',
    'state 985:',
    '',
    '	object_name : _CHARACTER _SET character_set_name _	(687)',
    '',
    '	.	reduce 687',
    '',
    'state 986:',
    '',
    '	column_name_list : column_name_list comma column_name _	(250)',
    '',
    '	.	reduce 250',
    '',
    'state 987:',
    '',
    '	insert_columns_and_source : left_paren insert_column_list right_paren _ query_expression',
    '',
    '	left_paren	shift 68',
    '	_SELECT	shift 83',
    '	_TABLE	shift 85',
    '	_VALUES	shift 87',
    '	.	error',
    '',
    '	explicit_table	goto 49',
    '	table_value_constructor	goto 50',
    '	query_specification	goto 51',
    '	table_subquery	goto 52',
    '	simple_table	goto 53',
    '	non_join_query_primary	goto 54',
    '	query_term	goto 55',
    '	non_join_query_term	goto 56',
    '	query_expression	goto 1160',
    '',
    'state 988:',
    '',
    '	module_name_clause : _MODULE _MODULE module_name _MODULE module_character_set_specification _MODULE _ module_name module_character_set_specification',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	module_name	goto 1161',
    '	actual_identifier	goto 61',
    '	identifier	goto 364',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 989:',
    '',
    '	module_character_set_specification : _NAMES _ARE _ character_set_specification',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	actual_identifier	goto 61',
    '	SQL_language_identifier	goto 96',
    '	identifier	goto 97',
    '	character_set_name	goto 98',
    '	character_set_specification	goto 1162',
    '	introducer	goto 63',
    '	regular_identifier	goto 100',
    '',
    'state 990:',
    '',
    '	revoke_statement : _REVOKE grant_option_for_opt privileges _ON object_name _FROM _ grantee_list drop_behaviour',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_PUBLIC	shift 984',
    '	.	error',
    '',
    '	grantee	goto 981',
    '	grantee_list	goto 1163',
    '	authorization_identifier	goto 983',
    '	actual_identifier	goto 61',
    '	identifier	goto 472',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 991:',
    '',
    '	group_by_clause_opt : group_by_clause _	(380)',
    '',
    '	.	reduce 380',
    '',
    'state 992:',
    '',
    '	table_expression : from_clause where_clause_opt group_by_clause_opt _ having_clause_opt',
    '	having_clause_opt : _	(381)',
    '',
    '	_HAVING	shift 1166',
    '	$end	reduce 381',
    '	identifier_body	reduce 381',
    '	delimited_identifier	reduce 381',
    '	left_paren	reduce 381',
    '	right_paren	reduce 381',
    '	semicolon	reduce 381',
    '	underscore	reduce 381',
    '	_ALTER	reduce 381',
    '	_COMMIT	reduce 381',
    '	_CONNECT	reduce 381',
    '	_CREATE	reduce 381',
    '	_DECLARE	reduce 381',
    '	_DELETE	reduce 381',
    '	_DISCONNECT	reduce 381',
    '	_DROP	reduce 381',
    '	_EXCEPT	reduce 381',
    '	_FOR	reduce 381',
    '	_GRANT	reduce 381',
    '	_INSERT	reduce 381',
    '	_INTERSECT	reduce 381',
    '	_ORDER	reduce 381',
    '	_REVOKE	reduce 381',
    '	_ROLLBACK	reduce 381',
    '	_SELECT	reduce 381',
    '	_SET	reduce 381',
    '	_TABLE	reduce 381',
    '	_UNION	reduce 381',
    '	_UPDATE	reduce 381',
    '	_VALUES	reduce 381',
    '	_WITH	reduce 381',
    '	.	error',
    '',
    '	having_clause	goto 1164',
    '	having_clause_opt	goto 1165',
    '',
    'state 993:',
    '',
    '	group_by_clause : _GROUP _ _BY grouping_column_reference_list',
    '',
    '	_BY	shift 1167',
    '	.	error',
    '',
    'state 994:',
    '',
    '	table_factor : derived_table correlation_specification _	(390)',
    '',
    '	.	reduce 390',
    '',
    'state 995:',
    '',
    '	*** conflicts:',
    '',
    '	shift 1169, reduce 396 on left_paren',
    '',
    '	correlation_specification : correlation_name _ derived_column_list_opt',
    '	derived_column_list_opt : _	(396)',
    '',
    '	left_paren	shift 1169',
    '	$end	reduce 396',
    '	identifier_body	reduce 396',
    '	delimited_identifier	reduce 396',
    '	right_paren	reduce 396',
    '	comma	reduce 396',
    '	semicolon	reduce 396',
    '	underscore	reduce 396',
    '	_ALTER	reduce 396',
    '	_COMMIT	reduce 396',
    '	_CONNECT	reduce 396',
    '	_CREATE	reduce 396',
    '	_CROSS	reduce 396',
    '	_DECLARE	reduce 396',
    '	_DELETE	reduce 396',
    '	_DISCONNECT	reduce 396',
    '	_DROP	reduce 396',
    '	_EXCEPT	reduce 396',
    '	_FOR	reduce 396',
    '	_FULL	reduce 396',
    '	_GRANT	reduce 396',
    '	_GROUP	reduce 396',
    '	_HAVING	reduce 396',
    '	_INNER	reduce 396',
    '	_INSERT	reduce 396',
    '	_INTERSECT	reduce 396',
    '	_JOIN	reduce 396',
    '	_LEFT	reduce 396',
    '	_NATURAL	reduce 396',
    '	_ON	reduce 396',
    '	_ORDER	reduce 396',
    '	_REVOKE	reduce 396',
    '	_RIGHT	reduce 396',
    '	_ROLLBACK	reduce 396',
    '	_SELECT	reduce 396',
    '	_SET	reduce 396',
    '	_TABLE	reduce 396',
    '	_UNION	reduce 396',
    '	_UPDATE	reduce 396',
    '	_USING	reduce 396',
    '	_VALUES	reduce 396',
    '	_WHERE	reduce 396',
    '	_WITH	reduce 396',
    '	.	error',
    '',
    '	derived_column_list_opt	goto 1168',
    '',
    'state 996:',
    '',
    '	correlation_name : identifier _	(336)',
    '',
    '	.	reduce 336',
    '',
    'state 997:',
    '',
    '	table_factor : derived_table _AS _ correlation_specification',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	correlation_specification	goto 1170',
    '	correlation_name	goto 995',
    '	actual_identifier	goto 61',
    '	identifier	goto 996',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 998:',
    '',
    '	cross_join : table_reference _CROSS _ _JOIN table_factor',
    '',
    '	_JOIN	shift 1171',
    '	.	error',
    '',
    'state 999:',
    '',
    '	qualified_join : table_reference _FULL _ outer_opt _JOIN table_factor join_specification',
    '	outer_opt : _	(416)',
    '',
    '	_OUTER	shift 1173',
    '	_JOIN	reduce 416',
    '	.	error',
    '',
    '	outer_opt	goto 1172',
    '',
    'state 1000:',
    '',
    '	qualified_join : table_reference _INNER _ _JOIN table_factor join_specification',
    '',
    '	_JOIN	shift 1174',
    '	.	error',
    '',
    'state 1001:',
    '',
    '	qualified_join : table_reference _JOIN _ table_factor join_specification',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	left_paren	shift 68',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	derived_table	goto 797',
    '	table_factor	goto 1175',
    '	table_subquery	goto 802',
    '	table_name	goto 803',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1002:',
    '',
    '	qualified_join : table_reference _LEFT _ outer_opt _JOIN table_factor join_specification',
    '	outer_opt : _	(416)',
    '',
    '	_OUTER	shift 1173',
    '	_JOIN	reduce 416',
    '	.	error',
    '',
    '	outer_opt	goto 1176',
    '',
    'state 1003:',
    '',
    '	qualified_join : table_reference _NATURAL _ _JOIN table_factor',
    '	qualified_join : table_reference _NATURAL _ _INNER _JOIN table_factor',
    '	qualified_join : table_reference _NATURAL _ _LEFT outer_opt _JOIN table_factor',
    '	qualified_join : table_reference _NATURAL _ _RIGHT outer_opt _JOIN table_factor',
    '	qualified_join : table_reference _NATURAL _ _FULL outer_opt _JOIN table_factor',
    '	qualified_join : table_reference _NATURAL _ _UNION _JOIN table_factor',
    '',
    '	_FULL	shift 1177',
    '	_INNER	shift 1178',
    '	_JOIN	shift 1179',
    '	_LEFT	shift 1180',
    '	_RIGHT	shift 1181',
    '	_UNION	shift 1182',
    '	.	error',
    '',
    'state 1004:',
    '',
    '	qualified_join : table_reference _RIGHT _ outer_opt _JOIN table_factor join_specification',
    '	outer_opt : _	(416)',
    '',
    '	_OUTER	shift 1173',
    '	_JOIN	reduce 416',
    '	.	error',
    '',
    '	outer_opt	goto 1183',
    '',
    'state 1005:',
    '',
    '	from_clause_opt : from_clause_opt comma _ table_reference',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	left_paren	shift 804',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	qualified_join	goto 795',
    '	cross_join	goto 796',
    '	derived_table	goto 797',
    '	table_factor	goto 798',
    '	joined_table	goto 799',
    '	table_reference	goto 1184',
    '	table_subquery	goto 802',
    '	table_name	goto 803',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1006:',
    '',
    '	table_factor : table_name correlation_specification _	(389)',
    '',
    '	.	reduce 389',
    '',
    'state 1007:',
    '',
    '	table_factor : table_name _AS _ correlation_specification',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	correlation_specification	goto 1185',
    '	correlation_name	goto 995',
    '	actual_identifier	goto 61',
    '	identifier	goto 996',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1008:',
    '',
    '	joined_table : left_paren joined_table _ right_paren',
    '	table_reference : joined_table _	(386)',
    '',
    '	right_paren	shift 1186',
    '	_CROSS	reduce 386',
    '	_FULL	reduce 386',
    '	_INNER	reduce 386',
    '	_JOIN	reduce 386',
    '	_LEFT	reduce 386',
    '	_NATURAL	reduce 386',
    '	_RIGHT	reduce 386',
    '	.	error',
    '',
    'state 1009:',
    '',
    '	cross_join : table_reference _ _CROSS _JOIN table_factor',
    '	qualified_join : table_reference _ _JOIN table_factor join_specification',
    '	qualified_join : table_reference _ _INNER _JOIN table_factor join_specification',
    '	qualified_join : table_reference _ _LEFT outer_opt _JOIN table_factor join_specification',
    '	qualified_join : table_reference _ _RIGHT outer_opt _JOIN table_factor join_specification',
    '	qualified_join : table_reference _ _FULL outer_opt _JOIN table_factor join_specification',
    '	qualified_join : table_reference _ _NATURAL _JOIN table_factor',
    '	qualified_join : table_reference _ _NATURAL _INNER _JOIN table_factor',
    '	qualified_join : table_reference _ _NATURAL _LEFT outer_opt _JOIN table_factor',
    '	qualified_join : table_reference _ _NATURAL _RIGHT outer_opt _JOIN table_factor',
    '	qualified_join : table_reference _ _NATURAL _FULL outer_opt _JOIN table_factor',
    '	qualified_join : table_reference _ _NATURAL _UNION _JOIN table_factor',
    '',
    '	_CROSS	shift 998',
    '	_FULL	shift 999',
    '	_INNER	shift 1000',
    '	_JOIN	shift 1001',
    '	_LEFT	shift 1002',
    '	_NATURAL	shift 1003',
    '	_RIGHT	shift 1004',
    '	.	error',
    '',
    'state 1010:',
    '',
    '	non_join_query_primary : table_subquery _	(361)',
    '	derived_table : table_subquery _	(399)',
    '',
    '	right_paren	reduce 361',
    '	_EXCEPT	reduce 361',
    '	_INTERSECT	reduce 361',
    '	_UNION	reduce 361',
    '	identifier_body	reduce 399',
    '	delimited_identifier	reduce 399',
    '	underscore	reduce 399',
    '	_AS	reduce 399',
    '	.	error',
    '',
    'state 1011:',
    '',
    '	qualified_name : identifier period identifier period _ identifier',
    '	qualified_name_trail_asterisk : identifier period identifier period _ asterisk',
    '	qualified_name_trail_asterisk : identifier period identifier period _ identifier period asterisk',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	asterisk	shift 1188',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	actual_identifier	goto 61',
    '	identifier	goto 1187',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1012:',
    '',
    '	level_of_isolation : _READ _COMMITTED _	(834)',
    '',
    '	.	reduce 834',
    '',
    'state 1013:',
    '',
    '	level_of_isolation : _READ _UNCOMMITTED _	(833)',
    '',
    '	.	reduce 833',
    '',
    'state 1014:',
    '',
    '	level_of_isolation : _REPEATABLE _READ _	(835)',
    '',
    '	.	reduce 835',
    '',
    'state 1015:',
    '',
    '	qualified_name : identifier period identifier period identifier _	(189)',
    '',
    '	.	reduce 189',
    '',
    'state 1016:',
    '',
    '	set_clause : object_column equals_operator update_source _	(818)',
    '',
    '	.	reduce 818',
    '',
    'state 1017:',
    '',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	update_source : expression _	(820)',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	$end	reduce 820',
    '	identifier_body	reduce 820',
    '	delimited_identifier	reduce 820',
    '	left_paren	reduce 820',
    '	comma	reduce 820',
    '	semicolon	reduce 820',
    '	underscore	reduce 820',
    '	_ALTER	reduce 820',
    '	_COMMIT	reduce 820',
    '	_CONNECT	reduce 820',
    '	_CREATE	reduce 820',
    '	_DECLARE	reduce 820',
    '	_DELETE	reduce 820',
    '	_DISCONNECT	reduce 820',
    '	_DROP	reduce 820',
    '	_GRANT	reduce 820',
    '	_INSERT	reduce 820',
    '	_REVOKE	reduce 820',
    '	_ROLLBACK	reduce 820',
    '	_SELECT	reduce 820',
    '	_SET	reduce 820',
    '	_TABLE	reduce 820',
    '	_UPDATE	reduce 820',
    '	_VALUES	reduce 820',
    '	_WHERE	reduce 820',
    '	.	error',
    '',
    'state 1018:',
    '',
    '	set_clause_list : set_clause_list comma set_clause _	(817)',
    '',
    '	.	reduce 817',
    '',
    'state 1019:',
    '',
    '	start_field : non_second_datetime_field left_paren precision right_paren _	(171)',
    '',
    '	.	reduce 171',
    '',
    'state 1020:',
    '',
    '	end_field : _SECOND left_paren _ precision right_paren',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	precision	goto 1189',
    '	unsigned_integer	goto 825',
    '',
    'state 1021:',
    '',
    '	*** conflicts:',
    '',
    '	shift 421, reduce 503 on concatenation_operator',
    '	shift 422, reduce 503 on plus_sign',
    '	shift 423, reduce 503 on minus_sign',
    '',
    '	time_zone_specifier : _TIME _ZONE expression _	(503)',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	$end	reduce 503',
    '	identifier_body	reduce 503',
    '	delimited_identifier	reduce 503',
    '	not_equals_operator	reduce 503',
    '	greater_than_or_equals_operator	reduce 503',
    '	less_than_or_equals_operator	reduce 503',
    '	left_paren	reduce 503',
    '	right_paren	reduce 503',
    '	asterisk	reduce 503',
    '	comma	reduce 503',
    '	solidus	reduce 503',
    '	semicolon	reduce 503',
    '	less_than_operator	reduce 503',
    '	equals_operator	reduce 503',
    '	greater_than_operator	reduce 503',
    '	underscore	reduce 503',
    '	_ALTER	reduce 503',
    '	_AND	reduce 503',
    '	_AS	reduce 503',
    '	_BETWEEN	reduce 503',
    '	_COMMIT	reduce 503',
    '	_CONNECT	reduce 503',
    '	_CREATE	reduce 503',
    '	_CROSS	reduce 503',
    '	_DECLARE	reduce 503',
    '	_DELETE	reduce 503',
    '	_DISCONNECT	reduce 503',
    '	_DROP	reduce 503',
    '	_ELSE	reduce 503',
    '	_END	reduce 503',
    '	_ESCAPE	reduce 503',
    '	_EXCEPT	reduce 503',
    '	_FOR	reduce 503',
    '	_FROM	reduce 503',
    '	_FULL	reduce 503',
    '	_GRANT	reduce 503',
    '	_GROUP	reduce 503',
    '	_HAVING	reduce 503',
    '	_IN	reduce 503',
    '	_INNER	reduce 503',
    '	_INSERT	reduce 503',
    '	_INTERSECT	reduce 503',
    '	_INTO	reduce 503',
    '	_IS	reduce 503',
    '	_JOIN	reduce 503',
    '	_LEFT	reduce 503',
    '	_LIKE	reduce 503',
    '	_MATCH	reduce 503',
    '	_NATURAL	reduce 503',
    '	_NOT	reduce 503',
    '	_OR	reduce 503',
    '	_ORDER	reduce 503',
    '	_OVERLAPS	reduce 503',
    '	_REVOKE	reduce 503',
    '	_RIGHT	reduce 503',
    '	_ROLLBACK	reduce 503',
    '	_SELECT	reduce 503',
    '	_SET	reduce 503',
    '	_TABLE	reduce 503',
    '	_THEN	reduce 503',
    '	_UNION	reduce 503',
    '	_UPDATE	reduce 503',
    '	_USING	reduce 503',
    '	_VALUES	reduce 503',
    '	_WHEN	reduce 503',
    '	_WHERE	reduce 503',
    '	_WITH	reduce 503',
    '	.	error',
    '',
    'state 1022:',
    '',
    '	single_datetime_field_opt : left_paren interval_leading_field_precision single_datetime_field_opt2 _ right_paren',
    '',
    '	right_paren	shift 1190',
    '	.	error',
    '',
    'state 1023:',
    '',
    '	single_datetime_field_opt2 : comma _ interval_fractional_seconds_precision',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	interval_fractional_seconds_precision	goto 1191',
    '	unsigned_integer	goto 1192',
    '',
    'state 1024:',
    '',
    '	simple_case : _CASE case_operand simple_when_clause else_clause_opt _END _	(449)',
    '',
    '	.	reduce 449',
    '',
    'state 1025:',
    '',
    '	simple_when_clause : _WHEN when_operand _THEN _ result',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	result	goto 1193',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 836',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 1026:',
    '',
    '	overlaps_predicate : row_value_constructor_1 _OVERLAPS row_value_constructor_2 _	(552)',
    '',
    '	.	reduce 552',
    '',
    'state 1027:',
    '',
    '	row_value_constructor_2 : row_value_constructor _	(554)',
    '',
    '	.	reduce 554',
    '',
    'state 1028:',
    '',
    '	like_predicate : expression _LIKE pattern _ like_predicate_escape_opt',
    '	like_predicate_escape_opt : _	(532)',
    '',
    '	_ESCAPE	shift 1195',
    '	$end	reduce 532',
    '	identifier_body	reduce 532',
    '	delimited_identifier	reduce 532',
    '	left_paren	reduce 532',
    '	right_paren	reduce 532',
    '	comma	reduce 532',
    '	semicolon	reduce 532',
    '	underscore	reduce 532',
    '	_ALTER	reduce 532',
    '	_AND	reduce 532',
    '	_COMMIT	reduce 532',
    '	_CONNECT	reduce 532',
    '	_CREATE	reduce 532',
    '	_CROSS	reduce 532',
    '	_DECLARE	reduce 532',
    '	_DELETE	reduce 532',
    '	_DISCONNECT	reduce 532',
    '	_DROP	reduce 532',
    '	_EXCEPT	reduce 532',
    '	_FOR	reduce 532',
    '	_FULL	reduce 532',
    '	_GRANT	reduce 532',
    '	_GROUP	reduce 532',
    '	_HAVING	reduce 532',
    '	_INNER	reduce 532',
    '	_INSERT	reduce 532',
    '	_INTERSECT	reduce 532',
    '	_IS	reduce 532',
    '	_JOIN	reduce 532',
    '	_LEFT	reduce 532',
    '	_NATURAL	reduce 532',
    '	_OR	reduce 532',
    '	_ORDER	reduce 532',
    '	_REVOKE	reduce 532',
    '	_RIGHT	reduce 532',
    '	_ROLLBACK	reduce 532',
    '	_SELECT	reduce 532',
    '	_SET	reduce 532',
    '	_TABLE	reduce 532',
    '	_THEN	reduce 532',
    '	_UNION	reduce 532',
    '	_UPDATE	reduce 532',
    '	_VALUES	reduce 532',
    '	_WHERE	reduce 532',
    '	_WITH	reduce 532',
    '	.	error',
    '',
    '	like_predicate_escape_opt	goto 1194',
    '',
    'state 1029:',
    '',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	pattern : expression _	(534)',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	$end	reduce 534',
    '	identifier_body	reduce 534',
    '	delimited_identifier	reduce 534',
    '	left_paren	reduce 534',
    '	right_paren	reduce 534',
    '	comma	reduce 534',
    '	semicolon	reduce 534',
    '	underscore	reduce 534',
    '	_ALTER	reduce 534',
    '	_AND	reduce 534',
    '	_COMMIT	reduce 534',
    '	_CONNECT	reduce 534',
    '	_CREATE	reduce 534',
    '	_CROSS	reduce 534',
    '	_DECLARE	reduce 534',
    '	_DELETE	reduce 534',
    '	_DISCONNECT	reduce 534',
    '	_DROP	reduce 534',
    '	_ESCAPE	reduce 534',
    '	_EXCEPT	reduce 534',
    '	_FOR	reduce 534',
    '	_FULL	reduce 534',
    '	_GRANT	reduce 534',
    '	_GROUP	reduce 534',
    '	_HAVING	reduce 534',
    '	_INNER	reduce 534',
    '	_INSERT	reduce 534',
    '	_INTERSECT	reduce 534',
    '	_IS	reduce 534',
    '	_JOIN	reduce 534',
    '	_LEFT	reduce 534',
    '	_NATURAL	reduce 534',
    '	_OR	reduce 534',
    '	_ORDER	reduce 534',
    '	_REVOKE	reduce 534',
    '	_RIGHT	reduce 534',
    '	_ROLLBACK	reduce 534',
    '	_SELECT	reduce 534',
    '	_SET	reduce 534',
    '	_TABLE	reduce 534',
    '	_THEN	reduce 534',
    '	_UNION	reduce 534',
    '	_UPDATE	reduce 534',
    '	_VALUES	reduce 534',
    '	_WHERE	reduce 534',
    '	_WITH	reduce 534',
    '	.	error',
    '',
    'state 1030:',
    '',
    '	like_predicate : expression _NOT _LIKE _ pattern like_predicate_escape_opt',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	pattern	goto 1196',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 1029',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 1031:',
    '',
    '	quantifier : some _	(540)',
    '',
    '	.	reduce 540',
    '',
    'state 1032:',
    '',
    '	quantifier : all _	(539)',
    '',
    '	.	reduce 539',
    '',
    'state 1033:',
    '',
    '	quantified_comparison_predicate : row_value_constructor comp_op quantifier _ table_subquery',
    '',
    '	left_paren	shift 68',
    '	.	error',
    '',
    '	table_subquery	goto 1197',
    '',
    'state 1034:',
    '',
    '	comparison_predicate : row_value_constructor comp_op row_value_constructor _	(287)',
    '',
    '	.	reduce 287',
    '',
    'state 1035:',
    '',
    '	all : _ALL _	(541)',
    '',
    '	.	reduce 541',
    '',
    'state 1036:',
    '',
    '	some : _ANY _	(543)',
    '',
    '	.	reduce 543',
    '',
    'state 1037:',
    '',
    '	some : _SOME _	(542)',
    '',
    '	.	reduce 542',
    '',
    'state 1038:',
    '',
    '	between_predicate : row_value_constructor _BETWEEN row_value_constructor _ _AND row_value_constructor',
    '',
    '	_AND	shift 1198',
    '	.	error',
    '',
    'state 1039:',
    '',
    '	in_predicate : row_value_constructor _IN in_predicate_value _	(524)',
    '',
    '	.	reduce 524',
    '',
    'state 1040:',
    '',
    '	in_predicate_value : table_subquery _	(526)',
    '',
    '	.	reduce 526',
    '',
    'state 1041:',
    '',
    '	table_subquery : left_paren _ query_expression right_paren',
    '	in_predicate_value : left_paren _ in_value_list right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 429',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SELECT	shift 83',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TABLE	shift 85',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_VALUES	shift 87',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	in_value_list	goto 1199',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	explicit_table	goto 49',
    '	table_value_constructor	goto 50',
    '	query_specification	goto 51',
    '	table_subquery	goto 52',
    '	simple_table	goto 53',
    '	non_join_query_primary	goto 54',
    '	query_term	goto 55',
    '	non_join_query_term	goto 56',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	query_expression	goto 101',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 1200',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 1042:',
    '',
    '	null_predicate : row_value_constructor _IS _NOT _ _NULL',
    '',
    '	_NULL	shift 1201',
    '	.	error',
    '',
    'state 1043:',
    '',
    '	null_predicate : row_value_constructor _IS _NULL _	(536)',
    '',
    '	.	reduce 536',
    '',
    'state 1044:',
    '',
    '	match_predicate : row_value_constructor _MATCH unique_opt _ partial_full_opt table_subquery',
    '	partial_full_opt : _	(549)',
    '',
    '	_FULL	shift 1203',
    '	_PARTIAL	shift 1204',
    '	left_paren	reduce 549',
    '	.	error',
    '',
    '	partial_full_opt	goto 1202',
    '',
    'state 1045:',
    '',
    '	unique_opt : _UNIQUE _	(548)',
    '',
    '	.	reduce 548',
    '',
    'state 1046:',
    '',
    '	between_predicate : row_value_constructor _NOT _BETWEEN _ row_value_constructor _AND row_value_constructor',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 248',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 238',
    '	row_value_constructor	goto 1205',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 1047:',
    '',
    '	in_predicate : row_value_constructor _NOT _IN _ in_predicate_value',
    '',
    '	left_paren	shift 1041',
    '	.	error',
    '',
    '	in_predicate_value	goto 1206',
    '	table_subquery	goto 1040',
    '',
    'state 1048:',
    '',
    '	boolean_test : boolean_primary _IS truth_value _	(273)',
    '',
    '	.	reduce 273',
    '',
    'state 1049:',
    '',
    '	truth_value : _FALSE _	(556)',
    '',
    '	.	reduce 556',
    '',
    'state 1050:',
    '',
    '	boolean_test : boolean_primary _IS _NOT _ truth_value',
    '',
    '	_FALSE	shift 1049',
    '	_TRUE	shift 1051',
    '	_UNKNOWN	shift 1052',
    '	.	error',
    '',
    '	truth_value	goto 1207',
    '',
    'state 1051:',
    '',
    '	truth_value : _TRUE _	(555)',
    '',
    '	.	reduce 555',
    '',
    'state 1052:',
    '',
    '	truth_value : _UNKNOWN _	(557)',
    '',
    '	.	reduce 557',
    '',
    'state 1053:',
    '',
    '	boolean_term : boolean_term _AND boolean_factor _	(269)',
    '',
    '	.	reduce 269',
    '',
    'state 1054:',
    '',
    '	search_condition : search_condition _OR boolean_term _	(267)',
    '	boolean_term : boolean_term _ _AND boolean_factor',
    '',
    '	_AND	shift 856',
    '	$end	reduce 267',
    '	identifier_body	reduce 267',
    '	delimited_identifier	reduce 267',
    '	left_paren	reduce 267',
    '	right_paren	reduce 267',
    '	comma	reduce 267',
    '	semicolon	reduce 267',
    '	underscore	reduce 267',
    '	_ALTER	reduce 267',
    '	_COMMIT	reduce 267',
    '	_CONNECT	reduce 267',
    '	_CREATE	reduce 267',
    '	_CROSS	reduce 267',
    '	_DECLARE	reduce 267',
    '	_DELETE	reduce 267',
    '	_DISCONNECT	reduce 267',
    '	_DROP	reduce 267',
    '	_EXCEPT	reduce 267',
    '	_FOR	reduce 267',
    '	_FULL	reduce 267',
    '	_GRANT	reduce 267',
    '	_GROUP	reduce 267',
    '	_HAVING	reduce 267',
    '	_INNER	reduce 267',
    '	_INSERT	reduce 267',
    '	_INTERSECT	reduce 267',
    '	_JOIN	reduce 267',
    '	_LEFT	reduce 267',
    '	_NATURAL	reduce 267',
    '	_OR	reduce 267',
    '	_ORDER	reduce 267',
    '	_REVOKE	reduce 267',
    '	_RIGHT	reduce 267',
    '	_ROLLBACK	reduce 267',
    '	_SELECT	reduce 267',
    '	_SET	reduce 267',
    '	_TABLE	reduce 267',
    '	_THEN	reduce 267',
    '	_UNION	reduce 267',
    '	_UPDATE	reduce 267',
    '	_VALUES	reduce 267',
    '	_WHERE	reduce 267',
    '	_WITH	reduce 267',
    '	.	error',
    '',
    'state 1055:',
    '',
    '	searched_when_clause : _WHEN search_condition _THEN result _	(458)',
    '',
    '	.	reduce 458',
    '',
    'state 1056:',
    '',
    '	boolean_primary : left_paren search_condition right_paren _	(276)',
    '',
    '	.	reduce 276',
    '',
    'state 1057:',
    '',
    '	cast_specification : _CAST left_paren cast_operand _AS cast_target _ right_paren',
    '',
    '	right_paren	shift 1208',
    '	.	error',
    '',
    'state 1058:',
    '',
    '	cast_target : domain_name _	(461)',
    '',
    '	.	reduce 461',
    '',
    'state 1059:',
    '',
    '	cast_target : data_type _	(462)',
    '',
    '	.	reduce 462',
    '',
    'state 1060:',
    '',
    '	expression_list : expression_list comma expression _	(446)',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	right_paren	reduce 446',
    '	comma	reduce 446',
    '	.	error',
    '',
    'state 1061:',
    '',
    '	form_of_use_conversion : _CONVERT left_paren expression _USING form_of_use_conversion_name _ right_paren',
    '',
    '	right_paren	shift 1209',
    '	.	error',
    '',
    'state 1062:',
    '',
    '	form_of_use_conversion_name : qualified_name _	(480)',
    '',
    '	.	reduce 480',
    '',
    'state 1063:',
    '',
    '	extract_expression : _EXTRACT left_paren extract_field _FROM extract_source _ right_paren',
    '',
    '	right_paren	shift 1210',
    '	.	error',
    '',
    'state 1064:',
    '',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	extract_source : expression _	(500)',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	right_paren	reduce 500',
    '	.	error',
    '',
    'state 1065:',
    '',
    '	case_abbreviation : _NULLIF left_paren expression comma expression _ right_paren',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '',
    '	concatenation_operator	shift 421',
    '	right_paren	shift 1211',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	.	error',
    '',
    'state 1066:',
    '',
    '	position_expression : _POSITION left_paren expression _IN expression _ right_paren',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '',
    '	concatenation_operator	shift 421',
    '	right_paren	shift 1212',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	.	error',
    '',
    'state 1067:',
    '',
    '	character_bit_substring_function : _SUBSTRING left_paren expression _FROM start_position _ for_strlength_opt right_paren',
    '	for_strlength_opt : _	(473)',
    '',
    '	_FOR	shift 1214',
    '	right_paren	reduce 473',
    '	.	error',
    '',
    '	for_strlength_opt	goto 1213',
    '',
    'state 1068:',
    '',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	start_position : expression _	(475)',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	right_paren	reduce 475',
    '	_FOR	reduce 475',
    '	.	error',
    '',
    'state 1069:',
    '',
    '	character_translation : _TRANSLATE left_paren expression _USING translation_name _ right_paren',
    '',
    '	right_paren	shift 1215',
    '	.	error',
    '',
    'state 1070:',
    '',
    '	trim_operands : trim_character _FROM trim_source _	(486)',
    '',
    '	.	reduce 486',
    '',
    'state 1071:',
    '',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	trim_source : expression _	(492)',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	right_paren	reduce 492',
    '	.	error',
    '',
    'state 1072:',
    '',
    '	trim_operands : trim_specification trim_character _FROM _ trim_source',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_source	goto 1216',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 1071',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 1073:',
    '',
    '	trim_operands : trim_specification _FROM trim_source _	(485)',
    '',
    '	.	reduce 485',
    '',
    'state 1074:',
    '',
    '	corresponding_column_list_opt : _BY left_paren corresponding_column_list _ right_paren',
    '',
    '	right_paren	shift 1217',
    '	.	error',
    '',
    'state 1075:',
    '',
    '	column_name_list : column_name_list _ comma column_name',
    '	corresponding_column_list : column_name_list _	(439)',
    '',
    '	comma	shift 786',
    '	right_paren	reduce 439',
    '	.	error',
    '',
    'state 1076:',
    '',
    '	declare_cursor : _DECLARE cursor_name insensitive_opt scroll_opt _ _CURSOR _FOR cursor_specification',
    '	dynamic_declare_cursor : _DECLARE cursor_name insensitive_opt scroll_opt _ _CURSOR _FOR statement_name',
    '',
    '	_CURSOR	shift 1218',
    '	.	error',
    '',
    'state 1077:',
    '',
    '	scroll_opt : _SCROLL _	(584)',
    '',
    '	.	reduce 584',
    '',
    'state 1078:',
    '',
    '	procedure : _PROCEDURE procedure_name parameter_declaration_list semicolon _ SQL_procedure_statement semicolon',
    '',
    '	_ALTER	shift 70',
    '	_CLOSE	shift 1235',
    '	_COMMIT	shift 71',
    '	_CONNECT	shift 72',
    '	_CREATE	shift 73',
    '	_DELETE	shift 1236',
    '	_DISCONNECT	shift 76',
    '	_DROP	shift 77',
    '	_FETCH	shift 1237',
    '	_GRANT	shift 78',
    '	_INSERT	shift 79',
    '	_OPEN	shift 1238',
    '	_REVOKE	shift 81',
    '	_ROLLBACK	shift 82',
    '	_SELECT	shift 1239',
    '	_SET	shift 84',
    '	_UPDATE	shift 1240',
    '	.	error',
    '',
    '	set_local_time_zone_statement	goto 8',
    '	set_session_authorization_identifier_statement	goto 9',
    '	set_names_statement	goto 10',
    '	set_schema_statement	goto 11',
    '	set_catalog_statement	goto 12',
    '	disconnect_statement	goto 13',
    '	set_connection_statement	goto 14',
    '	connect_statement	goto 15',
    '	rollback_statement	goto 16',
    '	commit_statement	goto 17',
    '	set_constraints_mode_statement	goto 18',
    '	set_transaction_statement	goto 19',
    '	update_statement__searched	goto 1219',
    '	update_statement__positioned	goto 1220',
    '	insert_statement	goto 1221',
    '	delete_statement__searched	goto 1222',
    '	delete_statement__positioned	goto 1223',
    '	SQL_data_change_statement	goto 1224',
    '	select_statement__single_row	goto 1225',
    '	close_statement	goto 1226',
    '	fetch_statement	goto 1227',
    '	open_statement	goto 1228',
    '	drop_assertion_statement	goto 23',
    '	drop_translation_statement	goto 24',
    '	drop_collation_statement	goto 25',
    '	drop_character_set_statement	goto 26',
    '	drop_domain_statement	goto 27',
    '	alter_domain_statement	goto 28',
    '	revoke_statement	goto 29',
    '	drop_view_statement	goto 30',
    '	drop_table_statement	goto 31',
    '	alter_table_statement	goto 32',
    '	drop_schema_statement	goto 33',
    '	assertion_definition	goto 34',
    '	translation_definition	goto 35',
    '	collation_definition	goto 36',
    '	character_set_definition	goto 37',
    '	domain_definition	goto 38',
    '	grant_statement	goto 39',
    '	view_definition	goto 40',
    '	table_definition	goto 41',
    '	schema_definition	goto 42',
    '	SQL_schema_manipulation_statement	goto 43',
    '	SQL_schema_definition_statement	goto 44',
    '	SQL_session_statement	goto 1229',
    '	SQL_connection_statement	goto 1230',
    '	SQL_transaction_statement	goto 1231',
    '	SQL_data_statement	goto 1232',
    '	SQL_schema_statement	goto 1233',
    '	SQL_procedure_statement	goto 1234',
    '',
    'state 1079:',
    '',
    '	parameter_declaration : status_parameter _	(610)',
    '',
    '	.	reduce 610',
    '',
    'state 1080:',
    '',
    '	parameter_declarations : parameter_declaration _	(607)',
    '',
    '	.	reduce 607',
    '',
    'state 1081:',
    '',
    '	parameter_declaration_list : left_paren parameter_declarations _ right_paren',
    '	parameter_declarations : parameter_declarations _ comma parameter_declaration',
    '',
    '	right_paren	shift 1241',
    '	comma	shift 1242',
    '	.	error',
    '',
    'state 1082:',
    '',
    '	parameter_declaration : parameter_name _ data_type',
    '',
    '	_BIT	shift 727',
    '	_CHAR	shift 728',
    '	_CHARACTER	shift 729',
    '	_DATE	shift 730',
    '	_DEC	shift 731',
    '	_DECIMAL	shift 732',
    '	_DOUBLE	shift 733',
    '	_FLOAT	shift 734',
    '	_INT	shift 735',
    '	_INTEGER	shift 736',
    '	_INTERVAL	shift 737',
    '	_NATIONAL	shift 738',
    '	_NCHAR	shift 739',
    '	_NUMERIC	shift 740',
    '	_REAL	shift 741',
    '	_SMALLINT	shift 742',
    '	_TIME	shift 743',
    '	_TIMESTAMP	shift 744',
    '	_VARCHAR	shift 745',
    '	.	error',
    '',
    '	approximate_numeric_type	goto 718',
    '	exact_numeric_type	goto 719',
    '	interval_type	goto 720',
    '	datetime_type	goto 721',
    '	numeric_type	goto 722',
    '	bit_string_type	goto 723',
    '	national_character_string_type	goto 724',
    '	character_string_type	goto 725',
    '	data_type	goto 1243',
    '',
    'state 1083:',
    '',
    '	status_parameter : _SQLCODE _	(611)',
    '',
    '	.	reduce 611',
    '',
    'state 1084:',
    '',
    '	status_parameter : _SQLSTATE _	(612)',
    '',
    '	.	reduce 612',
    '',
    'state 1085:',
    '',
    '	domain_constraint : constraint_name_definition_opt check_constraint_definition constraint_attributes_opt _	(650)',
    '',
    '	.	reduce 650',
    '',
    'state 1086:',
    '',
    '	check_constraint_definition : _CHECK left_paren _ search_condition right_paren',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 636',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXISTS	shift 637',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NOT	shift 638',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UNIQUE	shift 639',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	row_value_constructor_1	goto 617',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 618',
    '	row_value_constructor	goto 619',
    '	overlaps_predicate	goto 620',
    '	match_predicate	goto 621',
    '	unique_predicate	goto 622',
    '	exists_predicate	goto 623',
    '	quantified_comparison_predicate	goto 624',
    '	null_predicate	goto 625',
    '	like_predicate	goto 626',
    '	in_predicate	goto 627',
    '	between_predicate	goto 628',
    '	comparison_predicate	goto 629',
    '	predicate	goto 630',
    '	boolean_primary	goto 631',
    '	boolean_test	goto 632',
    '	boolean_factor	goto 633',
    '	boolean_term	goto 634',
    '	search_condition	goto 1244',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 1087:',
    '',
    '	column_definition_sel : domain_name _	(93)',
    '',
    '	.	reduce 93',
    '',
    'state 1088:',
    '',
    '	column_definition_sel : data_type _	(92)',
    '',
    '	.	reduce 92',
    '',
    'state 1089:',
    '',
    '	column_definition : column_name column_definition_sel _ default_clause_opt column_constraint_definition_opt collate_clause_opt',
    '	default_clause_opt : _	(94)',
    '',
    '	_DEFAULT	shift 697',
    '	$end	reduce 94',
    '	identifier_body	reduce 94',
    '	delimited_identifier	reduce 94',
    '	left_paren	reduce 94',
    '	right_paren	reduce 94',
    '	comma	reduce 94',
    '	semicolon	reduce 94',
    '	underscore	reduce 94',
    '	_ALTER	reduce 94',
    '	_CHECK	reduce 94',
    '	_COLLATE	reduce 94',
    '	_COMMIT	reduce 94',
    '	_CONNECT	reduce 94',
    '	_CONSTRAINT	reduce 94',
    '	_CREATE	reduce 94',
    '	_DECLARE	reduce 94',
    '	_DELETE	reduce 94',
    '	_DISCONNECT	reduce 94',
    '	_DROP	reduce 94',
    '	_GRANT	reduce 94',
    '	_INSERT	reduce 94',
    '	_NOT	reduce 94',
    '	_PRIMARY	reduce 94',
    '	_REFERENCES	reduce 94',
    '	_REVOKE	reduce 94',
    '	_ROLLBACK	reduce 94',
    '	_SELECT	reduce 94',
    '	_SET	reduce 94',
    '	_TABLE	reduce 94',
    '	_UNIQUE	reduce 94',
    '	_UPDATE	reduce 94',
    '	_VALUES	reduce 94',
    '	.	error',
    '',
    '	default_clause	goto 941',
    '	default_clause_opt	goto 1245',
    '',
    'state 1090:',
    '',
    '	table_constraint_definition : constraint_name_definition_opt table_constraint constraint_check_time_opt _	(569)',
    '',
    '	.	reduce 569',
    '',
    'state 1091:',
    '',
    '	unique_constraint_definition : unique_specification left_paren _ unique_column_list right_paren',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	unique_column_list	goto 1246',
    '	column_name_list	goto 1247',
    '	column_name	goto 551',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1092:',
    '',
    '	referential_constraint_definition : _FOREIGN _KEY _ left_paren referencing_columns right_paren references_specification',
    '',
    '	left_paren	shift 1248',
    '	.	error',
    '',
    'state 1093:',
    '',
    '	unique_specification : _PRIMARY _KEY _	(237)',
    '',
    '	.	reduce 237',
    '',
    'state 1094:',
    '',
    '	alter_column_action : drop_column_default_clause _	(753)',
    '',
    '	.	reduce 753',
    '',
    'state 1095:',
    '',
    '	alter_column_action : set_column_default_clause _	(752)',
    '',
    '	.	reduce 752',
    '',
    'state 1096:',
    '',
    '	alter_column_definition : _ALTER column_opt column_name alter_column_action _	(751)',
    '',
    '	.	reduce 751',
    '',
    'state 1097:',
    '',
    '	drop_column_default_clause : _DROP _ _DEFAULT',
    '',
    '	_DEFAULT	shift 1249',
    '	.	error',
    '',
    'state 1098:',
    '',
    '	set_column_default_clause : _SET _ default_clause',
    '',
    '	_DEFAULT	shift 697',
    '	.	error',
    '',
    '	default_clause	goto 1250',
    '',
    'state 1099:',
    '',
    '	drop_column_definition : _DROP column_opt column_name drop_behaviour _	(756)',
    '',
    '	.	reduce 756',
    '',
    'state 1100:',
    '',
    '	drop_table_constraint_definition : _DROP _CONSTRAINT constraint_name drop_behaviour _	(758)',
    '',
    '	.	reduce 758',
    '',
    'state 1101:',
    '',
    '	table_commit_opts : _ON _COMMIT _ _DELETE _ROWS',
    '	table_commit_opts : _ON _COMMIT _ _PRESERVE _ROWS',
    '',
    '	_DELETE	shift 1251',
    '	_PRESERVE	shift 1252',
    '	.	error',
    '',
    'state 1102:',
    '',
    '	table_element_list : left_paren table_element table_element_list_opt _ right_paren',
    '	table_element_list_opt : table_element_list_opt _ comma table_element',
    '',
    '	right_paren	shift 1253',
    '	comma	shift 1254',
    '	.	error',
    '',
    'state 1103:',
    '',
    '	deferrable_opt : _NOT _DEFERRABLE _	(564)',
    '',
    '	.	reduce 564',
    '',
    'state 1104:',
    '',
    '	assertion_check : _CHECK left_paren search_condition right_paren _	(694)',
    '',
    '	.	reduce 694',
    '',
    'state 1105:',
    '',
    '	charset_collation_opt : limited_collation_definition _	(698)',
    '',
    '	.	reduce 698',
    '',
    'state 1106:',
    '',
    '	character_set_definition : _CREATE _CHARACTER _SET character_set_name as_opt character_set_source charset_collation_opt _	(695)',
    '',
    '	.	reduce 695',
    '',
    'state 1107:',
    '',
    '	charset_collation_opt : collate_clause _	(697)',
    '',
    '	.	reduce 697',
    '',
    'state 1108:',
    '',
    '	limited_collation_definition : _COLLATION _ _FROM collation_source',
    '',
    '	_FROM	shift 1255',
    '	.	error',
    '',
    'state 1109:',
    '',
    '	character_set_source : _GET existing_character_set_name _	(699)',
    '',
    '	.	reduce 699',
    '',
    'state 1110:',
    '',
    '	existing_character_set_name : character_set_name _	(700)',
    '',
    '	.	reduce 700',
    '',
    'state 1111:',
    '',
    '	collating_sequence_definition : schema_collation_name _	(705)',
    '',
    '	.	reduce 705',
    '',
    'state 1112:',
    '',
    '	collating_sequence_definition : external_collation _	(704)',
    '',
    '	.	reduce 704',
    '',
    'state 1113:',
    '',
    '	collation_source : translation_collation _	(703)',
    '',
    '	.	reduce 703',
    '',
    'state 1114:',
    '',
    '	collation_source : collating_sequence_definition _	(702)',
    '',
    '	.	reduce 702',
    '',
    'state 1115:',
    '',
    '	collation_definition : _CREATE _COLLATION collation_name _FOR character_set_specification _FROM collation_source _ pad_attribute_opt',
    '	pad_attribute_opt : _	(715)',
    '',
    '	_NO	shift 1257',
    '	_PAD	shift 1258',
    '	$end	reduce 715',
    '	identifier_body	reduce 715',
    '	delimited_identifier	reduce 715',
    '	left_paren	reduce 715',
    '	semicolon	reduce 715',
    '	underscore	reduce 715',
    '	_ALTER	reduce 715',
    '	_COMMIT	reduce 715',
    '	_CONNECT	reduce 715',
    '	_CREATE	reduce 715',
    '	_DECLARE	reduce 715',
    '	_DELETE	reduce 715',
    '	_DISCONNECT	reduce 715',
    '	_DROP	reduce 715',
    '	_GRANT	reduce 715',
    '	_INSERT	reduce 715',
    '	_REVOKE	reduce 715',
    '	_ROLLBACK	reduce 715',
    '	_SELECT	reduce 715',
    '	_SET	reduce 715',
    '	_TABLE	reduce 715',
    '	_UPDATE	reduce 715',
    '	_VALUES	reduce 715',
    '	.	error',
    '',
    '	pad_attribute_opt	goto 1256',
    '',
    'state 1116:',
    '',
    '	schema_collation_name : collation_name _	(710)',
    '',
    '	.	reduce 710',
    '',
    'state 1117:',
    '',
    '	collating_sequence_definition : _DEFAULT _	(707)',
    '',
    '	.	reduce 707',
    '',
    'state 1118:',
    '',
    '	collating_sequence_definition : _DESC _ left_paren collation_name right_paren',
    '',
    '	left_paren	shift 1259',
    '	.	error',
    '',
    'state 1119:',
    '',
    '	external_collation : _EXTERNAL _ left_paren quote external_collation_name quote right_paren',
    '',
    '	left_paren	shift 1260',
    '	.	error',
    '',
    'state 1120:',
    '',
    '	translation_collation : _TRANSLATION _ translation_name translation_collation_opt',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	translation_name	goto 1261',
    '	qualified_name	goto 322',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1121:',
    '',
    '	data_type_opt : _CHARACTER _SET _ character_set_specification',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	actual_identifier	goto 61',
    '	SQL_language_identifier	goto 96',
    '	identifier	goto 97',
    '	character_set_name	goto 98',
    '	character_set_specification	goto 1262',
    '	introducer	goto 63',
    '	regular_identifier	goto 100',
    '',
    'state 1122:',
    '',
    '	domain_constraint_opt : domain_constraint _	(649)',
    '',
    '	.	reduce 649',
    '',
    'state 1123:',
    '',
    '	domain_definition : _CREATE _DOMAIN domain_name as_opt data_type default_clause_opt domain_constraint_opt _ collate_clause_opt',
    '	collate_clause_opt : _	(98)',
    '',
    '	_COLLATE	shift 414',
    '	$end	reduce 98',
    '	identifier_body	reduce 98',
    '	delimited_identifier	reduce 98',
    '	left_paren	reduce 98',
    '	semicolon	reduce 98',
    '	underscore	reduce 98',
    '	_ALTER	reduce 98',
    '	_COMMIT	reduce 98',
    '	_CONNECT	reduce 98',
    '	_CREATE	reduce 98',
    '	_DECLARE	reduce 98',
    '	_DELETE	reduce 98',
    '	_DISCONNECT	reduce 98',
    '	_DROP	reduce 98',
    '	_GRANT	reduce 98',
    '	_INSERT	reduce 98',
    '	_REVOKE	reduce 98',
    '	_ROLLBACK	reduce 98',
    '	_SELECT	reduce 98',
    '	_SET	reduce 98',
    '	_TABLE	reduce 98',
    '	_UPDATE	reduce 98',
    '	_VALUES	reduce 98',
    '	.	error',
    '',
    '	collate_clause	goto 678',
    '	collate_clause_opt	goto 1263',
    '',
    'state 1124:',
    '',
    '	character_string_type_len : left_paren length _ right_paren',
    '',
    '	right_paren	shift 1264',
    '	.	error',
    '',
    'state 1125:',
    '',
    '	unsigned_integer : unsigned_integer _ digit',
    '	length : unsigned_integer _	(120)',
    '',
    '	digit	shift 331',
    '	right_paren	reduce 120',
    '	.	error',
    '',
    'state 1126:',
    '',
    '	bit_string_type : _BIT _VARYING character_string_type_len _	(134)',
    '',
    '	.	reduce 134',
    '',
    'state 1127:',
    '',
    '	character_string_type : _CHAR _VARYING character_string_type_len _	(112)',
    '',
    '	.	reduce 112',
    '',
    'state 1128:',
    '',
    '	character_string_type : _CHARACTER _VARYING character_string_type_len _	(111)',
    '',
    '	.	reduce 111',
    '',
    'state 1129:',
    '',
    '	numeric_precision_scale_opt : left_paren precision _ comma scale right_paren',
    '	numeric_precision_scale_opt : left_paren precision _ right_paren',
    '',
    '	right_paren	shift 1265',
    '	comma	shift 1266',
    '	.	error',
    '',
    'state 1130:',
    '',
    '	approximate_numeric_type : _FLOAT left_paren precision _ right_paren',
    '',
    '	right_paren	shift 1267',
    '	.	error',
    '',
    'state 1131:',
    '',
    '	national_character_string_type : _NATIONAL _CHAR character_string_type_len _	(122)',
    '',
    '	.	reduce 122',
    '',
    'state 1132:',
    '',
    '	*** conflicts:',
    '',
    '	shift 944, reduce 131 on left_paren',
    '',
    '	national_character_string_type : _NATIONAL _CHAR _VARYING _ character_string_type_len',
    '	national_character_string_type : _NATIONAL _CHAR _VARYING _	(131)',
    '',
    '	left_paren	shift 944',
    '	$end	reduce 131',
    '	identifier_body	reduce 131',
    '	delimited_identifier	reduce 131',
    '	right_paren	reduce 131',
    '	comma	reduce 131',
    '	semicolon	reduce 131',
    '	underscore	reduce 131',
    '	_ALTER	reduce 131',
    '	_CHECK	reduce 131',
    '	_COLLATE	reduce 131',
    '	_COMMIT	reduce 131',
    '	_CONNECT	reduce 131',
    '	_CONSTRAINT	reduce 131',
    '	_CREATE	reduce 131',
    '	_DECLARE	reduce 131',
    '	_DEFAULT	reduce 131',
    '	_DELETE	reduce 131',
    '	_DISCONNECT	reduce 131',
    '	_DROP	reduce 131',
    '	_GRANT	reduce 131',
    '	_INSERT	reduce 131',
    '	_NOT	reduce 131',
    '	_PRIMARY	reduce 131',
    '	_REFERENCES	reduce 131',
    '	_REVOKE	reduce 131',
    '	_ROLLBACK	reduce 131',
    '	_SELECT	reduce 131',
    '	_SET	reduce 131',
    '	_TABLE	reduce 131',
    '	_UNIQUE	reduce 131',
    '	_UPDATE	reduce 131',
    '	_VALUES	reduce 131',
    '	.	error',
    '',
    '	character_string_type_len	goto 1268',
    '',
    'state 1133:',
    '',
    '	national_character_string_type : _NATIONAL _CHARACTER character_string_type_len _	(121)',
    '',
    '	.	reduce 121',
    '',
    'state 1134:',
    '',
    '	*** conflicts:',
    '',
    '	shift 944, reduce 130 on left_paren',
    '',
    '	national_character_string_type : _NATIONAL _CHARACTER _VARYING _ character_string_type_len',
    '	national_character_string_type : _NATIONAL _CHARACTER _VARYING _	(130)',
    '',
    '	left_paren	shift 944',
    '	$end	reduce 130',
    '	identifier_body	reduce 130',
    '	delimited_identifier	reduce 130',
    '	right_paren	reduce 130',
    '	comma	reduce 130',
    '	semicolon	reduce 130',
    '	underscore	reduce 130',
    '	_ALTER	reduce 130',
    '	_CHECK	reduce 130',
    '	_COLLATE	reduce 130',
    '	_COMMIT	reduce 130',
    '	_CONNECT	reduce 130',
    '	_CONSTRAINT	reduce 130',
    '	_CREATE	reduce 130',
    '	_DECLARE	reduce 130',
    '	_DEFAULT	reduce 130',
    '	_DELETE	reduce 130',
    '	_DISCONNECT	reduce 130',
    '	_DROP	reduce 130',
    '	_GRANT	reduce 130',
    '	_INSERT	reduce 130',
    '	_NOT	reduce 130',
    '	_PRIMARY	reduce 130',
    '	_REFERENCES	reduce 130',
    '	_REVOKE	reduce 130',
    '	_ROLLBACK	reduce 130',
    '	_SELECT	reduce 130',
    '	_SET	reduce 130',
    '	_TABLE	reduce 130',
    '	_UNIQUE	reduce 130',
    '	_UPDATE	reduce 130',
    '	_VALUES	reduce 130',
    '	.	error',
    '',
    '	character_string_type_len	goto 1269',
    '',
    'state 1135:',
    '',
    '	national_character_string_type : _NCHAR _VARYING character_string_type_len _	(126)',
    '',
    '	.	reduce 126',
    '',
    'state 1136:',
    '',
    '	datetime_type : _TIME time_precision_opt tz_opt _	(155)',
    '',
    '	.	reduce 155',
    '',
    'state 1137:',
    '',
    '	tz_opt : _WITH _ _TIME _ZONE',
    '',
    '	_TIME	shift 1270',
    '	.	error',
    '',
    'state 1138:',
    '',
    '	time_precision_opt : left_paren time_precision _ right_paren',
    '',
    '	right_paren	shift 1271',
    '	.	error',
    '',
    'state 1139:',
    '',
    '	datetime_type : _TIMESTAMP timestamp_precision_opt tz_opt _	(156)',
    '',
    '	.	reduce 156',
    '',
    'state 1140:',
    '',
    '	timestamp_precision_opt : left_paren timestamp_precision _ right_paren',
    '',
    '	right_paren	shift 1272',
    '	.	error',
    '',
    'state 1141:',
    '',
    '	schema_character_set_specification : _DEFAULT _CHARACTER _SET character_set_specification _	(638)',
    '',
    '	.	reduce 638',
    '',
    'state 1142:',
    '',
    '	translation_definition : _CREATE _TRANSLATION translation_name _FOR source_character_set_specification _TO target_character_set_specification _ _FROM translation_source',
    '',
    '	_FROM	shift 1273',
    '	.	error',
    '',
    'state 1143:',
    '',
    '	target_character_set_specification : character_set_specification _	(720)',
    '',
    '	.	reduce 720',
    '',
    'state 1144:',
    '',
    '	view_definition : _CREATE _VIEW table_name view_column_list_opt _AS query_expression view_check_opt _	(658)',
    '',
    '	.	reduce 658',
    '',
    'state 1145:',
    '',
    '	view_check_opt : _WITH _ _CHECK _OPTION',
    '	view_check_opt : _WITH _ _CASCADED _CHECK _OPTION',
    '	view_check_opt : _WITH _ _LOCAL _CHECK _OPTION',
    '',
    '	_CASCADED	shift 1274',
    '	_CHECK	shift 1275',
    '	_LOCAL	shift 1276',
    '	.	error',
    '',
    'state 1146:',
    '',
    '	temporary_table_declaration : _DECLARE _LOCAL _TEMPORARY _TABLE qualified_local_table_name table_element_list temporary_table_declaration_opt _	(80)',
    '',
    '	.	reduce 80',
    '',
    'state 1147:',
    '',
    '	temporary_table_declaration_opt : _ON _ _COMMIT _PRESERVE _ROWS',
    '	temporary_table_declaration_opt : _ON _ _COMMIT _DELETE _ROWS',
    '',
    '	_COMMIT	shift 1277',
    '	.	error',
    '',
    'state 1148:',
    '',
    '	date_value : unsigned_integer minus_sign unsigned_integer minus_sign _ unsigned_integer',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	unsigned_integer	goto 1278',
    '',
    'state 1149:',
    '',
    '	interval_string_literal : unsigned_integer space unsigned_integer colon _ unsigned_integer',
    '	interval_string_literal : unsigned_integer space unsigned_integer colon _ unsigned_integer colon seconds_value',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	unsigned_integer	goto 1279',
    '',
    'state 1150:',
    '',
    '	seconds_value : unsigned_integer period _ unsigned_integer',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	unsigned_integer	goto 1280',
    '',
    'state 1151:',
    '',
    '	interval_string_literal : unsigned_integer colon unsigned_integer colon _ seconds_value',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	seconds_value	goto 1281',
    '	unsigned_integer	goto 1282',
    '',
    'state 1152:',
    '',
    '	time_string : quote time_value quote quote time_value _ time_zone_interval quote',
    '',
    '	plus_sign	shift 148',
    '	minus_sign	shift 149',
    '	.	error',
    '',
    '	time_zone_interval	goto 1283',
    '	sign	goto 1155',
    '',
    'state 1153:',
    '',
    '	time_value : unsigned_integer colon unsigned_integer colon _ seconds_value',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	seconds_value	goto 1284',
    '	unsigned_integer	goto 1282',
    '',
    'state 1154:',
    '',
    '	timestamp_string : quote date_value space time_value time_zone_interval _ quote',
    '',
    '	quote	shift 1285',
    '	.	error',
    '',
    'state 1155:',
    '',
    '	time_zone_interval : sign _ unsigned_integer colon unsigned_integer',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	unsigned_integer	goto 1286',
    '',
    'state 1156:',
    '',
    '	timestamp_string : quote date_value space time_value quote _	(50)',
    '',
    '	.	reduce 50',
    '',
    'state 1157:',
    '',
    '	grant_statement : _GRANT privileges _ON object_name _TO grantee_list grant_option _	(666)',
    '',
    '	.	reduce 666',
    '',
    'state 1158:',
    '',
    '	grantee_list : grantee_list comma _ grantee',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_PUBLIC	shift 984',
    '	.	error',
    '',
    '	grantee	goto 1287',
    '	authorization_identifier	goto 983',
    '	actual_identifier	goto 61',
    '	identifier	goto 472',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1159:',
    '',
    '	grant_option : _WITH _ _GRANT _OPTION',
    '',
    '	_GRANT	shift 1288',
    '	.	error',
    '',
    'state 1160:',
    '',
    '	insert_columns_and_source : left_paren insert_column_list right_paren query_expression _	(811)',
    '	query_expression : query_expression _ _UNION all_opt corresponding_spec_opt query_term',
    '	query_expression : query_expression _ _EXCEPT all_opt corresponding_spec_opt query_term',
    '',
    '	_EXCEPT	shift 91',
    '	_UNION	shift 93',
    '	$end	reduce 811',
    '	identifier_body	reduce 811',
    '	delimited_identifier	reduce 811',
    '	left_paren	reduce 811',
    '	semicolon	reduce 811',
    '	underscore	reduce 811',
    '	_ALTER	reduce 811',
    '	_COMMIT	reduce 811',
    '	_CONNECT	reduce 811',
    '	_CREATE	reduce 811',
    '	_DECLARE	reduce 811',
    '	_DELETE	reduce 811',
    '	_DISCONNECT	reduce 811',
    '	_DROP	reduce 811',
    '	_GRANT	reduce 811',
    '	_INSERT	reduce 811',
    '	_REVOKE	reduce 811',
    '	_ROLLBACK	reduce 811',
    '	_SELECT	reduce 811',
    '	_SET	reduce 811',
    '	_TABLE	reduce 811',
    '	_UPDATE	reduce 811',
    '	_VALUES	reduce 811',
    '	.	error',
    '',
    'state 1161:',
    '',
    '	module_name_clause : _MODULE _MODULE module_name _MODULE module_character_set_specification _MODULE module_name _ module_character_set_specification',
    '',
    '	_NAMES	shift 791',
    '	.	error',
    '',
    '	module_character_set_specification	goto 1289',
    '',
    'state 1162:',
    '',
    '	module_character_set_specification : _NAMES _ARE character_set_specification _	(66)',
    '',
    '	.	reduce 66',
    '',
    'state 1163:',
    '',
    '	revoke_statement : _REVOKE grant_option_for_opt privileges _ON object_name _FROM grantee_list _ drop_behaviour',
    '	grantee_list : grantee_list _ comma grantee',
    '',
    '	comma	shift 1158',
    '	_CASCADE	shift 536',
    '	_RESTRICT	shift 537',
    '	.	error',
    '',
    '	drop_behaviour	goto 1290',
    '',
    'state 1164:',
    '',
    '	having_clause_opt : having_clause _	(382)',
    '',
    '	.	reduce 382',
    '',
    'state 1165:',
    '',
    '	table_expression : from_clause where_clause_opt group_by_clause_opt having_clause_opt _	(376)',
    '',
    '	.	reduce 376',
    '',
    'state 1166:',
    '',
    '	having_clause : _HAVING _ search_condition',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 636',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXISTS	shift 637',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NOT	shift 638',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UNIQUE	shift 639',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	row_value_constructor_1	goto 617',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 618',
    '	row_value_constructor	goto 619',
    '	overlaps_predicate	goto 620',
    '	match_predicate	goto 621',
    '	unique_predicate	goto 622',
    '	exists_predicate	goto 623',
    '	quantified_comparison_predicate	goto 624',
    '	null_predicate	goto 625',
    '	like_predicate	goto 626',
    '	in_predicate	goto 627',
    '	between_predicate	goto 628',
    '	comparison_predicate	goto 629',
    '	predicate	goto 630',
    '	boolean_primary	goto 631',
    '	boolean_test	goto 632',
    '	boolean_factor	goto 633',
    '	boolean_term	goto 634',
    '	search_condition	goto 1291',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 1167:',
    '',
    '	group_by_clause : _GROUP _BY _ grouping_column_reference_list',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	grouping_column_reference	goto 1292',
    '	grouping_column_reference_list	goto 1293',
    '	column_reference	goto 1294',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1168:',
    '',
    '	correlation_specification : correlation_name derived_column_list_opt _	(393)',
    '',
    '	.	reduce 393',
    '',
    'state 1169:',
    '',
    '	derived_column_list_opt : left_paren _ derived_column_list right_paren',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	derived_column_list	goto 1295',
    '	column_name_list	goto 1296',
    '	column_name	goto 551',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1170:',
    '',
    '	table_factor : derived_table _AS correlation_specification _	(392)',
    '',
    '	.	reduce 392',
    '',
    'state 1171:',
    '',
    '	cross_join : table_reference _CROSS _JOIN _ table_factor',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	left_paren	shift 68',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	derived_table	goto 797',
    '	table_factor	goto 1297',
    '	table_subquery	goto 802',
    '	table_name	goto 803',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1172:',
    '',
    '	qualified_join : table_reference _FULL outer_opt _ _JOIN table_factor join_specification',
    '',
    '	_JOIN	shift 1298',
    '	.	error',
    '',
    'state 1173:',
    '',
    '	outer_opt : _OUTER _	(417)',
    '',
    '	.	reduce 417',
    '',
    'state 1174:',
    '',
    '	qualified_join : table_reference _INNER _JOIN _ table_factor join_specification',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	left_paren	shift 68',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	derived_table	goto 797',
    '	table_factor	goto 1299',
    '	table_subquery	goto 802',
    '	table_name	goto 803',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1175:',
    '',
    '	qualified_join : table_reference _JOIN table_factor _ join_specification',
    '',
    '	_ON	shift 1303',
    '	_USING	shift 1304',
    '	.	error',
    '',
    '	named_columns_join	goto 1300',
    '	join_condition	goto 1301',
    '	join_specification	goto 1302',
    '',
    'state 1176:',
    '',
    '	qualified_join : table_reference _LEFT outer_opt _ _JOIN table_factor join_specification',
    '',
    '	_JOIN	shift 1305',
    '	.	error',
    '',
    'state 1177:',
    '',
    '	qualified_join : table_reference _NATURAL _FULL _ outer_opt _JOIN table_factor',
    '	outer_opt : _	(416)',
    '',
    '	_OUTER	shift 1173',
    '	_JOIN	reduce 416',
    '	.	error',
    '',
    '	outer_opt	goto 1306',
    '',
    'state 1178:',
    '',
    '	qualified_join : table_reference _NATURAL _INNER _ _JOIN table_factor',
    '',
    '	_JOIN	shift 1307',
    '	.	error',
    '',
    'state 1179:',
    '',
    '	qualified_join : table_reference _NATURAL _JOIN _ table_factor',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	left_paren	shift 68',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	derived_table	goto 797',
    '	table_factor	goto 1308',
    '	table_subquery	goto 802',
    '	table_name	goto 803',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1180:',
    '',
    '	qualified_join : table_reference _NATURAL _LEFT _ outer_opt _JOIN table_factor',
    '	outer_opt : _	(416)',
    '',
    '	_OUTER	shift 1173',
    '	_JOIN	reduce 416',
    '	.	error',
    '',
    '	outer_opt	goto 1309',
    '',
    'state 1181:',
    '',
    '	qualified_join : table_reference _NATURAL _RIGHT _ outer_opt _JOIN table_factor',
    '	outer_opt : _	(416)',
    '',
    '	_OUTER	shift 1173',
    '	_JOIN	reduce 416',
    '	.	error',
    '',
    '	outer_opt	goto 1310',
    '',
    'state 1182:',
    '',
    '	qualified_join : table_reference _NATURAL _UNION _ _JOIN table_factor',
    '',
    '	_JOIN	shift 1311',
    '	.	error',
    '',
    'state 1183:',
    '',
    '	qualified_join : table_reference _RIGHT outer_opt _ _JOIN table_factor join_specification',
    '',
    '	_JOIN	shift 1312',
    '	.	error',
    '',
    'state 1184:',
    '',
    '	from_clause_opt : from_clause_opt comma table_reference _	(385)',
    '	cross_join : table_reference _ _CROSS _JOIN table_factor',
    '	qualified_join : table_reference _ _JOIN table_factor join_specification',
    '	qualified_join : table_reference _ _INNER _JOIN table_factor join_specification',
    '	qualified_join : table_reference _ _LEFT outer_opt _JOIN table_factor join_specification',
    '	qualified_join : table_reference _ _RIGHT outer_opt _JOIN table_factor join_specification',
    '	qualified_join : table_reference _ _FULL outer_opt _JOIN table_factor join_specification',
    '	qualified_join : table_reference _ _NATURAL _JOIN table_factor',
    '	qualified_join : table_reference _ _NATURAL _INNER _JOIN table_factor',
    '	qualified_join : table_reference _ _NATURAL _LEFT outer_opt _JOIN table_factor',
    '	qualified_join : table_reference _ _NATURAL _RIGHT outer_opt _JOIN table_factor',
    '	qualified_join : table_reference _ _NATURAL _FULL outer_opt _JOIN table_factor',
    '	qualified_join : table_reference _ _NATURAL _UNION _JOIN table_factor',
    '',
    '	_CROSS	shift 998',
    '	_FULL	shift 999',
    '	_INNER	shift 1000',
    '	_JOIN	shift 1001',
    '	_LEFT	shift 1002',
    '	_NATURAL	shift 1003',
    '	_RIGHT	shift 1004',
    '	$end	reduce 385',
    '	identifier_body	reduce 385',
    '	delimited_identifier	reduce 385',
    '	left_paren	reduce 385',
    '	right_paren	reduce 385',
    '	comma	reduce 385',
    '	semicolon	reduce 385',
    '	underscore	reduce 385',
    '	_ALTER	reduce 385',
    '	_COMMIT	reduce 385',
    '	_CONNECT	reduce 385',
    '	_CREATE	reduce 385',
    '	_DECLARE	reduce 385',
    '	_DELETE	reduce 385',
    '	_DISCONNECT	reduce 385',
    '	_DROP	reduce 385',
    '	_EXCEPT	reduce 385',
    '	_FOR	reduce 385',
    '	_GRANT	reduce 385',
    '	_GROUP	reduce 385',
    '	_HAVING	reduce 385',
    '	_INSERT	reduce 385',
    '	_INTERSECT	reduce 385',
    '	_ORDER	reduce 385',
    '	_REVOKE	reduce 385',
    '	_ROLLBACK	reduce 385',
    '	_SELECT	reduce 385',
    '	_SET	reduce 385',
    '	_TABLE	reduce 385',
    '	_UNION	reduce 385',
    '	_UPDATE	reduce 385',
    '	_VALUES	reduce 385',
    '	_WHERE	reduce 385',
    '	_WITH	reduce 385',
    '	.	error',
    '',
    'state 1185:',
    '',
    '	table_factor : table_name _AS correlation_specification _	(391)',
    '',
    '	.	reduce 391',
    '',
    'state 1186:',
    '',
    '	joined_table : left_paren joined_table right_paren _	(403)',
    '',
    '	.	reduce 403',
    '',
    'state 1187:',
    '',
    '	qualified_name : identifier period identifier period identifier _	(189)',
    '	qualified_name_trail_asterisk : identifier period identifier period identifier _ period asterisk',
    '',
    '	period	shift 1313',
    '	identifier_body	reduce 189',
    '	delimited_identifier	reduce 189',
    '	concatenation_operator	reduce 189',
    '	asterisk	reduce 189',
    '	plus_sign	reduce 189',
    '	comma	reduce 189',
    '	minus_sign	reduce 189',
    '	solidus	reduce 189',
    '	underscore	reduce 189',
    '	_AS	reduce 189',
    '	_AT	reduce 189',
    '	_COLLATE	reduce 189',
    '	_DAY	reduce 189',
    '	_FROM	reduce 189',
    '	_HOUR	reduce 189',
    '	_INTO	reduce 189',
    '	_MINUTE	reduce 189',
    '	_MONTH	reduce 189',
    '	_SECOND	reduce 189',
    '	_YEAR	reduce 189',
    '	.	error',
    '',
    'state 1188:',
    '',
    '	qualified_name_trail_asterisk : identifier period identifier period asterisk _	(191)',
    '',
    '	.	reduce 191',
    '',
    'state 1189:',
    '',
    '	end_field : _SECOND left_paren precision _ right_paren',
    '',
    '	right_paren	shift 1314',
    '	.	error',
    '',
    'state 1190:',
    '',
    '	single_datetime_field_opt : left_paren interval_leading_field_precision single_datetime_field_opt2 right_paren _	(183)',
    '',
    '	.	reduce 183',
    '',
    'state 1191:',
    '',
    '	single_datetime_field_opt2 : comma interval_fractional_seconds_precision _	(185)',
    '',
    '	.	reduce 185',
    '',
    'state 1192:',
    '',
    '	unsigned_integer : unsigned_integer _ digit',
    '	interval_fractional_seconds_precision : unsigned_integer _	(181)',
    '',
    '	digit	shift 331',
    '	right_paren	reduce 181',
    '	.	error',
    '',
    'state 1193:',
    '',
    '	simple_when_clause : _WHEN when_operand _THEN result _	(453)',
    '',
    '	.	reduce 453',
    '',
    'state 1194:',
    '',
    '	like_predicate : expression _LIKE pattern like_predicate_escape_opt _	(530)',
    '',
    '	.	reduce 530',
    '',
    'state 1195:',
    '',
    '	like_predicate_escape_opt : _ESCAPE _ escape_character',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	escape_character	goto 1315',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 1316',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 1196:',
    '',
    '	like_predicate : expression _NOT _LIKE pattern _ like_predicate_escape_opt',
    '	like_predicate_escape_opt : _	(532)',
    '',
    '	_ESCAPE	shift 1195',
    '	$end	reduce 532',
    '	identifier_body	reduce 532',
    '	delimited_identifier	reduce 532',
    '	left_paren	reduce 532',
    '	right_paren	reduce 532',
    '	comma	reduce 532',
    '	semicolon	reduce 532',
    '	underscore	reduce 532',
    '	_ALTER	reduce 532',
    '	_AND	reduce 532',
    '	_COMMIT	reduce 532',
    '	_CONNECT	reduce 532',
    '	_CREATE	reduce 532',
    '	_CROSS	reduce 532',
    '	_DECLARE	reduce 532',
    '	_DELETE	reduce 532',
    '	_DISCONNECT	reduce 532',
    '	_DROP	reduce 532',
    '	_EXCEPT	reduce 532',
    '	_FOR	reduce 532',
    '	_FULL	reduce 532',
    '	_GRANT	reduce 532',
    '	_GROUP	reduce 532',
    '	_HAVING	reduce 532',
    '	_INNER	reduce 532',
    '	_INSERT	reduce 532',
    '	_INTERSECT	reduce 532',
    '	_IS	reduce 532',
    '	_JOIN	reduce 532',
    '	_LEFT	reduce 532',
    '	_NATURAL	reduce 532',
    '	_OR	reduce 532',
    '	_ORDER	reduce 532',
    '	_REVOKE	reduce 532',
    '	_RIGHT	reduce 532',
    '	_ROLLBACK	reduce 532',
    '	_SELECT	reduce 532',
    '	_SET	reduce 532',
    '	_TABLE	reduce 532',
    '	_THEN	reduce 532',
    '	_UNION	reduce 532',
    '	_UPDATE	reduce 532',
    '	_VALUES	reduce 532',
    '	_WHERE	reduce 532',
    '	_WITH	reduce 532',
    '	.	error',
    '',
    '	like_predicate_escape_opt	goto 1317',
    '',
    'state 1197:',
    '',
    '	quantified_comparison_predicate : row_value_constructor comp_op quantifier table_subquery _	(538)',
    '',
    '	.	reduce 538',
    '',
    'state 1198:',
    '',
    '	between_predicate : row_value_constructor _BETWEEN row_value_constructor _AND _ row_value_constructor',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 248',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 238',
    '	row_value_constructor	goto 1318',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 1199:',
    '',
    '	in_predicate_value : left_paren in_value_list _ right_paren',
    '	in_value_list : in_value_list _ comma expression',
    '',
    '	right_paren	shift 1319',
    '	comma	shift 1320',
    '	.	error',
    '',
    'state 1200:',
    '',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	in_value_list : expression _	(528)',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	right_paren	reduce 528',
    '	comma	reduce 528',
    '	.	error',
    '',
    'state 1201:',
    '',
    '	null_predicate : row_value_constructor _IS _NOT _NULL _	(537)',
    '',
    '	.	reduce 537',
    '',
    'state 1202:',
    '',
    '	match_predicate : row_value_constructor _MATCH unique_opt partial_full_opt _ table_subquery',
    '',
    '	left_paren	shift 68',
    '	.	error',
    '',
    '	table_subquery	goto 1321',
    '',
    'state 1203:',
    '',
    '	partial_full_opt : _FULL _	(551)',
    '',
    '	.	reduce 551',
    '',
    'state 1204:',
    '',
    '	partial_full_opt : _PARTIAL _	(550)',
    '',
    '	.	reduce 550',
    '',
    'state 1205:',
    '',
    '	between_predicate : row_value_constructor _NOT _BETWEEN row_value_constructor _ _AND row_value_constructor',
    '',
    '	_AND	shift 1322',
    '	.	error',
    '',
    'state 1206:',
    '',
    '	in_predicate : row_value_constructor _NOT _IN in_predicate_value _	(525)',
    '',
    '	.	reduce 525',
    '',
    'state 1207:',
    '',
    '	boolean_test : boolean_primary _IS _NOT truth_value _	(274)',
    '',
    '	.	reduce 274',
    '',
    'state 1208:',
    '',
    '	cast_specification : _CAST left_paren cast_operand _AS cast_target right_paren _	(459)',
    '',
    '	.	reduce 459',
    '',
    'state 1209:',
    '',
    '	form_of_use_conversion : _CONVERT left_paren expression _USING form_of_use_conversion_name right_paren _	(479)',
    '',
    '	.	reduce 479',
    '',
    'state 1210:',
    '',
    '	extract_expression : _EXTRACT left_paren extract_field _FROM extract_source right_paren _	(493)',
    '',
    '	.	reduce 493',
    '',
    'state 1211:',
    '',
    '	case_abbreviation : _NULLIF left_paren expression comma expression right_paren _	(443)',
    '',
    '	.	reduce 443',
    '',
    'state 1212:',
    '',
    '	position_expression : _POSITION left_paren expression _IN expression right_paren _	(466)',
    '',
    '	.	reduce 466',
    '',
    'state 1213:',
    '',
    '	character_bit_substring_function : _SUBSTRING left_paren expression _FROM start_position for_strlength_opt _ right_paren',
    '',
    '	right_paren	shift 1323',
    '	.	error',
    '',
    'state 1214:',
    '',
    '	for_strlength_opt : _FOR _ string_length',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	string_length	goto 1324',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 1325',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 1215:',
    '',
    '	character_translation : _TRANSLATE left_paren expression _USING translation_name right_paren _	(481)',
    '',
    '	.	reduce 481',
    '',
    'state 1216:',
    '',
    '	trim_operands : trim_specification trim_character _FROM trim_source _	(487)',
    '',
    '	.	reduce 487',
    '',
    'state 1217:',
    '',
    '	corresponding_column_list_opt : _BY left_paren corresponding_column_list right_paren _	(438)',
    '',
    '	.	reduce 438',
    '',
    'state 1218:',
    '',
    '	declare_cursor : _DECLARE cursor_name insensitive_opt scroll_opt _CURSOR _ _FOR cursor_specification',
    '	dynamic_declare_cursor : _DECLARE cursor_name insensitive_opt scroll_opt _CURSOR _ _FOR statement_name',
    '',
    '	_FOR	shift 1326',
    '	.	error',
    '',
    'state 1219:',
    '',
    '	SQL_data_change_statement : update_statement__searched _	(807)',
    '',
    '	.	reduce 807',
    '',
    'state 1220:',
    '',
    '	SQL_data_change_statement : update_statement__positioned _	(806)',
    '',
    '	.	reduce 806',
    '',
    'state 1221:',
    '',
    '	SQL_data_change_statement : insert_statement _	(805)',
    '',
    '	.	reduce 805',
    '',
    'state 1222:',
    '',
    '	SQL_data_change_statement : delete_statement__searched _	(804)',
    '',
    '	.	reduce 804',
    '',
    'state 1223:',
    '',
    '	SQL_data_change_statement : delete_statement__positioned _	(803)',
    '',
    '	.	reduce 803',
    '',
    'state 1224:',
    '',
    '	SQL_data_statement : SQL_data_change_statement _	(782)',
    '',
    '	.	reduce 782',
    '',
    'state 1225:',
    '',
    '	SQL_data_statement : select_statement__single_row _	(781)',
    '',
    '	.	reduce 781',
    '',
    'state 1226:',
    '',
    '	SQL_data_statement : close_statement _	(780)',
    '',
    '	.	reduce 780',
    '',
    'state 1227:',
    '',
    '	SQL_data_statement : fetch_statement _	(779)',
    '',
    '	.	reduce 779',
    '',
    'state 1228:',
    '',
    '	SQL_data_statement : open_statement _	(778)',
    '',
    '	.	reduce 778',
    '',
    'state 1229:',
    '',
    '	SQL_procedure_statement : SQL_session_statement _	(617)',
    '',
    '	.	reduce 617',
    '',
    'state 1230:',
    '',
    '	SQL_procedure_statement : SQL_connection_statement _	(616)',
    '',
    '	.	reduce 616',
    '',
    'state 1231:',
    '',
    '	SQL_procedure_statement : SQL_transaction_statement _	(615)',
    '',
    '	.	reduce 615',
    '',
    'state 1232:',
    '',
    '	SQL_procedure_statement : SQL_data_statement _	(614)',
    '',
    '	.	reduce 614',
    '',
    'state 1233:',
    '',
    '	SQL_procedure_statement : SQL_schema_statement _	(613)',
    '',
    '	.	reduce 613',
    '',
    'state 1234:',
    '',
    '	procedure : _PROCEDURE procedure_name parameter_declaration_list semicolon SQL_procedure_statement _ semicolon',
    '',
    '	semicolon	shift 1327',
    '	.	error',
    '',
    'state 1235:',
    '',
    '	close_statement : _CLOSE _ cursor_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	cursor_name	goto 1328',
    '	actual_identifier	goto 61',
    '	identifier	goto 685',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1236:',
    '',
    '	delete_statement__positioned : _DELETE _ _FROM table_name _WHERE _CURRENT _OF cursor_name',
    '	delete_statement__searched : _DELETE _ _FROM table_name where_clause_opt',
    '',
    '	_FROM	shift 1329',
    '	.	error',
    '',
    'state 1237:',
    '',
    '	fetch_statement : _FETCH _ fetch_orientation_opt cursor_name _INTO fetch_target_list',
    '	fetch_orientation_opt : _	(785)',
    '',
    '	_ABSOLUTE	shift 1332',
    '	_FIRST	shift 1333',
    '	_FROM	shift 1334',
    '	_LAST	shift 1335',
    '	_NEXT	shift 1336',
    '	_PRIOR	shift 1337',
    '	_RELATIVE	shift 1338',
    '	identifier_body	reduce 785',
    '	delimited_identifier	reduce 785',
    '	underscore	reduce 785',
    '	.	error',
    '',
    '	fetch_orientation	goto 1330',
    '	fetch_orientation_opt	goto 1331',
    '',
    'state 1238:',
    '',
    '	open_statement : _OPEN _ cursor_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	cursor_name	goto 1339',
    '	actual_identifier	goto 61',
    '	identifier	goto 685',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1239:',
    '',
    '	select_statement__single_row : _SELECT _ set_quantifier_opt select_list _INTO select_target_list table_expression',
    '	set_quantifier_opt : _	(349)',
    '',
    '	_ALL	shift 184',
    '	_DISTINCT	shift 185',
    '	identifier_body	reduce 349',
    '	national_character_string_literal_start	reduce 349',
    '	bit_string_literal_start	reduce 349',
    '	string_literal_continuation	reduce 349',
    '	hex_string_literal_start	reduce 349',
    '	delimited_identifier	reduce 349',
    '	digit	reduce 349',
    '	left_paren	reduce 349',
    '	asterisk	reduce 349',
    '	plus_sign	reduce 349',
    '	minus_sign	reduce 349',
    '	period	reduce 349',
    '	colon	reduce 349',
    '	underscore	reduce 349',
    '	_AVG	reduce 349',
    '	_BIT_LENGTH	reduce 349',
    '	_CASE	reduce 349',
    '	_CAST	reduce 349',
    '	_CHARACTER_LENGTH	reduce 349',
    '	_CHAR_LENGTH	reduce 349',
    '	_COALESCE	reduce 349',
    '	_CONVERT	reduce 349',
    '	_CURRENT_DATE	reduce 349',
    '	_CURRENT_TIME	reduce 349',
    '	_CURRENT_TIMESTAMP	reduce 349',
    '	_CURRENT_USER	reduce 349',
    '	_DATE	reduce 349',
    '	_DEFAULT	reduce 349',
    '	_EXTRACT	reduce 349',
    '	_INTERVAL	reduce 349',
    '	_LOWER	reduce 349',
    '	_MAX	reduce 349',
    '	_MIN	reduce 349',
    '	_NULL	reduce 349',
    '	_NULLIF	reduce 349',
    '	_OCTET_LENGTH	reduce 349',
    '	_POSITION	reduce 349',
    '	_SESSION_USER	reduce 349',
    '	_SUBSTRING	reduce 349',
    '	_SUM	reduce 349',
    '	_SYSTEM_USER	reduce 349',
    '	_TIME	reduce 349',
    '	_TIMESTAMP	reduce 349',
    '	_TRANSLATE	reduce 349',
    '	_TRIM	reduce 349',
    '	_UPPER	reduce 349',
    '	_USER	reduce 349',
    '	_VALUE	reduce 349',
    '	_COUNT	reduce 349',
    '	.	error',
    '',
    '	set_quantifier_opt	goto 1340',
    '	set_quantifier	goto 183',
    '',
    'state 1240:',
    '',
    '	update_statement__positioned : _UPDATE _ table_name _SET set_clause_list _WHERE _CURRENT _OF cursor_name',
    '	update_statement__searched : _UPDATE _ table_name _SET set_clause_list where_clause_opt',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	table_name	goto 1341',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1241:',
    '',
    '	parameter_declaration_list : left_paren parameter_declarations right_paren _	(606)',
    '',
    '	.	reduce 606',
    '',
    'state 1242:',
    '',
    '	parameter_declarations : parameter_declarations comma _ parameter_declaration',
    '',
    '	colon	shift 151',
    '	_SQLCODE	shift 1083',
    '	_SQLSTATE	shift 1084',
    '	.	error',
    '',
    '	status_parameter	goto 1079',
    '	parameter_declaration	goto 1342',
    '	parameter_name	goto 1082',
    '',
    'state 1243:',
    '',
    '	parameter_declaration : parameter_name data_type _	(609)',
    '',
    '	.	reduce 609',
    '',
    'state 1244:',
    '',
    '	check_constraint_definition : _CHECK left_paren search_condition _ right_paren',
    '	search_condition : search_condition _ _OR boolean_term',
    '',
    '	right_paren	shift 1343',
    '	_OR	shift 857',
    '	.	error',
    '',
    'state 1245:',
    '',
    '	column_definition : column_name column_definition_sel default_clause_opt _ column_constraint_definition_opt collate_clause_opt',
    '	column_constraint_definition_opt : _	(96)',
    '	constraint_name_definition_opt : _	(229)',
    '',
    '	_CONSTRAINT	shift 693',
    '	$end	reduce 96',
    '	identifier_body	reduce 96',
    '	delimited_identifier	reduce 96',
    '	left_paren	reduce 96',
    '	right_paren	reduce 96',
    '	comma	reduce 96',
    '	semicolon	reduce 96',
    '	underscore	reduce 96',
    '	_ALTER	reduce 96',
    '	_COLLATE	reduce 96',
    '	_COMMIT	reduce 96',
    '	_CONNECT	reduce 96',
    '	_CREATE	reduce 96',
    '	_DECLARE	reduce 96',
    '	_DELETE	reduce 96',
    '	_DISCONNECT	reduce 96',
    '	_DROP	reduce 96',
    '	_GRANT	reduce 96',
    '	_INSERT	reduce 96',
    '	_REVOKE	reduce 96',
    '	_ROLLBACK	reduce 96',
    '	_SELECT	reduce 96',
    '	_SET	reduce 96',
    '	_TABLE	reduce 96',
    '	_UPDATE	reduce 96',
    '	_VALUES	reduce 96',
    '	_CHECK	reduce 229',
    '	_NOT	reduce 229',
    '	_PRIMARY	reduce 229',
    '	_REFERENCES	reduce 229',
    '	_UNIQUE	reduce 229',
    '	.	error',
    '',
    '	constraint_name_definition	goto 691',
    '	constraint_name_definition_opt	goto 1344',
    '	column_constraint_definition	goto 1345',
    '	column_constraint_definition_opt	goto 1346',
    '',
    'state 1246:',
    '',
    '	unique_constraint_definition : unique_specification left_paren unique_column_list _ right_paren',
    '',
    '	right_paren	shift 1347',
    '	.	error',
    '',
    'state 1247:',
    '',
    '	column_name_list : column_name_list _ comma column_name',
    '	unique_column_list : column_name_list _	(574)',
    '',
    '	comma	shift 786',
    '	right_paren	reduce 574',
    '	.	error',
    '',
    'state 1248:',
    '',
    '	referential_constraint_definition : _FOREIGN _KEY left_paren _ referencing_columns right_paren references_specification',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	referencing_columns	goto 1348',
    '	column_name_list	goto 1349',
    '	reference_column_list	goto 1350',
    '	column_name	goto 551',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1249:',
    '',
    '	drop_column_default_clause : _DROP _DEFAULT _	(755)',
    '',
    '	.	reduce 755',
    '',
    'state 1250:',
    '',
    '	set_column_default_clause : _SET default_clause _	(754)',
    '',
    '	.	reduce 754',
    '',
    'state 1251:',
    '',
    '	table_commit_opts : _ON _COMMIT _DELETE _ _ROWS',
    '',
    '	_ROWS	shift 1351',
    '	.	error',
    '',
    'state 1252:',
    '',
    '	table_commit_opts : _ON _COMMIT _PRESERVE _ _ROWS',
    '',
    '	_ROWS	shift 1352',
    '	.	error',
    '',
    'state 1253:',
    '',
    '	table_element_list : left_paren table_element table_element_list_opt right_paren _	(86)',
    '',
    '	.	reduce 86',
    '',
    'state 1254:',
    '',
    '	table_element_list_opt : table_element_list_opt comma _ table_element',
    '	constraint_name_definition_opt : _	(229)',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_CONSTRAINT	shift 693',
    '	_CHECK	reduce 229',
    '	_FOREIGN	reduce 229',
    '	_PRIMARY	reduce 229',
    '	_UNIQUE	reduce 229',
    '	.	error',
    '',
    '	constraint_name_definition	goto 691',
    '	constraint_name_definition_opt	goto 699',
    '	column_name	goto 908',
    '	table_constraint_definition	goto 925',
    '	column_definition	goto 926',
    '	table_element	goto 1353',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1255:',
    '',
    '	limited_collation_definition : _COLLATION _FROM _ collation_source',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_DEFAULT	shift 1117',
    '	_DESC	shift 1118',
    '	_EXTERNAL	shift 1119',
    '	_TRANSLATION	shift 1120',
    '	.	error',
    '',
    '	schema_collation_name	goto 1111',
    '	external_collation	goto 1112',
    '	translation_collation	goto 1113',
    '	collating_sequence_definition	goto 1114',
    '	collation_source	goto 1354',
    '	collation_name	goto 1116',
    '	qualified_name	goto 313',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1256:',
    '',
    '	collation_definition : _CREATE _COLLATION collation_name _FOR character_set_specification _FROM collation_source pad_attribute_opt _	(714)',
    '',
    '	.	reduce 714',
    '',
    'state 1257:',
    '',
    '	pad_attribute_opt : _NO _ _PAD',
    '',
    '	_PAD	shift 1355',
    '	.	error',
    '',
    'state 1258:',
    '',
    '	pad_attribute_opt : _PAD _ _SPACE',
    '',
    '	_SPACE	shift 1356',
    '	.	error',
    '',
    'state 1259:',
    '',
    '	collating_sequence_definition : _DESC left_paren _ collation_name right_paren',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	collation_name	goto 1357',
    '	qualified_name	goto 313',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1260:',
    '',
    '	external_collation : _EXTERNAL left_paren _ quote external_collation_name quote right_paren',
    '',
    '	quote	shift 1358',
    '	.	error',
    '',
    'state 1261:',
    '',
    '	translation_collation : _TRANSLATION translation_name _ translation_collation_opt',
    '	translation_collation_opt : _	(712)',
    '',
    '	_THEN	shift 1360',
    '	$end	reduce 712',
    '	identifier_body	reduce 712',
    '	delimited_identifier	reduce 712',
    '	left_paren	reduce 712',
    '	semicolon	reduce 712',
    '	underscore	reduce 712',
    '	_ALTER	reduce 712',
    '	_COMMIT	reduce 712',
    '	_CONNECT	reduce 712',
    '	_CREATE	reduce 712',
    '	_DECLARE	reduce 712',
    '	_DELETE	reduce 712',
    '	_DISCONNECT	reduce 712',
    '	_DROP	reduce 712',
    '	_GRANT	reduce 712',
    '	_INSERT	reduce 712',
    '	_NO	reduce 712',
    '	_PAD	reduce 712',
    '	_REVOKE	reduce 712',
    '	_ROLLBACK	reduce 712',
    '	_SELECT	reduce 712',
    '	_SET	reduce 712',
    '	_TABLE	reduce 712',
    '	_UPDATE	reduce 712',
    '	_VALUES	reduce 712',
    '	.	error',
    '',
    '	translation_collation_opt	goto 1359',
    '',
    'state 1262:',
    '',
    '	data_type_opt : _CHARACTER _SET character_set_specification _	(108)',
    '',
    '	.	reduce 108',
    '',
    'state 1263:',
    '',
    '	domain_definition : _CREATE _DOMAIN domain_name as_opt data_type default_clause_opt domain_constraint_opt collate_clause_opt _	(647)',
    '',
    '	.	reduce 647',
    '',
    'state 1264:',
    '',
    '	character_string_type_len : left_paren length right_paren _	(119)',
    '',
    '	.	reduce 119',
    '',
    'state 1265:',
    '',
    '	numeric_precision_scale_opt : left_paren precision right_paren _	(147)',
    '',
    '	.	reduce 147',
    '',
    'state 1266:',
    '',
    '	numeric_precision_scale_opt : left_paren precision comma _ scale right_paren',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	scale	goto 1361',
    '	unsigned_integer	goto 1362',
    '',
    'state 1267:',
    '',
    '	approximate_numeric_type : _FLOAT left_paren precision right_paren _	(151)',
    '',
    '	.	reduce 151',
    '',
    'state 1268:',
    '',
    '	national_character_string_type : _NATIONAL _CHAR _VARYING character_string_type_len _	(125)',
    '',
    '	.	reduce 125',
    '',
    'state 1269:',
    '',
    '	national_character_string_type : _NATIONAL _CHARACTER _VARYING character_string_type_len _	(124)',
    '',
    '	.	reduce 124',
    '',
    'state 1270:',
    '',
    '	tz_opt : _WITH _TIME _ _ZONE',
    '',
    '	_ZONE	shift 1363',
    '	.	error',
    '',
    'state 1271:',
    '',
    '	time_precision_opt : left_paren time_precision right_paren _	(160)',
    '',
    '	.	reduce 160',
    '',
    'state 1272:',
    '',
    '	timestamp_precision_opt : left_paren timestamp_precision right_paren _	(158)',
    '',
    '	.	reduce 158',
    '',
    'state 1273:',
    '',
    '	translation_definition : _CREATE _TRANSLATION translation_name _FOR source_character_set_specification _TO target_character_set_specification _FROM _ translation_source',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_EXTERNAL	shift 1369',
    '	_IDENTITY	shift 1370',
    '	.	error',
    '',
    '	schema_translation_name	goto 1364',
    '	external_translation	goto 1365',
    '	translation_specification	goto 1366',
    '	translation_source	goto 1367',
    '	translation_name	goto 1368',
    '	qualified_name	goto 322',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1274:',
    '',
    '	view_check_opt : _WITH _CASCADED _ _CHECK _OPTION',
    '',
    '	_CHECK	shift 1371',
    '	.	error',
    '',
    'state 1275:',
    '',
    '	view_check_opt : _WITH _CHECK _ _OPTION',
    '',
    '	_OPTION	shift 1372',
    '	.	error',
    '',
    'state 1276:',
    '',
    '	view_check_opt : _WITH _LOCAL _ _CHECK _OPTION',
    '',
    '	_CHECK	shift 1373',
    '	.	error',
    '',
    'state 1277:',
    '',
    '	temporary_table_declaration_opt : _ON _COMMIT _ _PRESERVE _ROWS',
    '	temporary_table_declaration_opt : _ON _COMMIT _ _DELETE _ROWS',
    '',
    '	_DELETE	shift 1374',
    '	_PRESERVE	shift 1375',
    '	.	error',
    '',
    'state 1278:',
    '',
    '	date_value : unsigned_integer minus_sign unsigned_integer minus_sign unsigned_integer _	(44)',
    '	unsigned_integer : unsigned_integer _ digit',
    '',
    '	digit	shift 331',
    '	space	reduce 44',
    '	quote	reduce 44',
    '	.	error',
    '',
    'state 1279:',
    '',
    '	interval_string_literal : unsigned_integer space unsigned_integer colon unsigned_integer _	(56)',
    '	interval_string_literal : unsigned_integer space unsigned_integer colon unsigned_integer _ colon seconds_value',
    '	unsigned_integer : unsigned_integer _ digit',
    '',
    '	digit	shift 331',
    '	colon	shift 1376',
    '	quote	reduce 56',
    '	.	error',
    '',
    'state 1280:',
    '',
    '	seconds_value : unsigned_integer period unsigned_integer _	(48)',
    '	unsigned_integer : unsigned_integer _ digit',
    '',
    '	digit	shift 331',
    '	quote	reduce 48',
    '	plus_sign	reduce 48',
    '	minus_sign	reduce 48',
    '	.	error',
    '',
    'state 1281:',
    '',
    '	interval_string_literal : unsigned_integer colon unsigned_integer colon seconds_value _	(60)',
    '',
    '	.	reduce 60',
    '',
    'state 1282:',
    '',
    '	unsigned_integer : unsigned_integer _ digit',
    '	seconds_value : unsigned_integer _	(47)',
    '	seconds_value : unsigned_integer _ period unsigned_integer',
    '',
    '	digit	shift 331',
    '	period	shift 1150',
    '	quote	reduce 47',
    '	plus_sign	reduce 47',
    '	minus_sign	reduce 47',
    '	.	error',
    '',
    'state 1283:',
    '',
    '	time_string : quote time_value quote quote time_value time_zone_interval _ quote',
    '',
    '	quote	shift 1377',
    '	.	error',
    '',
    'state 1284:',
    '',
    '	time_value : unsigned_integer colon unsigned_integer colon seconds_value _	(46)',
    '',
    '	.	reduce 46',
    '',
    'state 1285:',
    '',
    '	timestamp_string : quote date_value space time_value time_zone_interval quote _	(51)',
    '',
    '	.	reduce 51',
    '',
    'state 1286:',
    '',
    '	time_zone_interval : sign unsigned_integer _ colon unsigned_integer',
    '	unsigned_integer : unsigned_integer _ digit',
    '',
    '	digit	shift 331',
    '	colon	shift 1378',
    '	.	error',
    '',
    'state 1287:',
    '',
    '	grantee_list : grantee_list comma grantee _	(668)',
    '',
    '	.	reduce 668',
    '',
    'state 1288:',
    '',
    '	grant_option : _WITH _GRANT _ _OPTION',
    '',
    '	_OPTION	shift 1379',
    '	.	error',
    '',
    'state 1289:',
    '',
    '	module_name_clause : _MODULE _MODULE module_name _MODULE module_character_set_specification _MODULE module_name module_character_set_specification _	(64)',
    '',
    '	.	reduce 64',
    '',
    'state 1290:',
    '',
    '	revoke_statement : _REVOKE grant_option_for_opt privileges _ON object_name _FROM grantee_list drop_behaviour _	(761)',
    '',
    '	.	reduce 761',
    '',
    'state 1291:',
    '',
    '	having_clause : _HAVING search_condition _	(430)',
    '	search_condition : search_condition _ _OR boolean_term',
    '',
    '	_OR	shift 857',
    '	$end	reduce 430',
    '	identifier_body	reduce 430',
    '	delimited_identifier	reduce 430',
    '	left_paren	reduce 430',
    '	right_paren	reduce 430',
    '	semicolon	reduce 430',
    '	underscore	reduce 430',
    '	_ALTER	reduce 430',
    '	_COMMIT	reduce 430',
    '	_CONNECT	reduce 430',
    '	_CREATE	reduce 430',
    '	_DECLARE	reduce 430',
    '	_DELETE	reduce 430',
    '	_DISCONNECT	reduce 430',
    '	_DROP	reduce 430',
    '	_EXCEPT	reduce 430',
    '	_FOR	reduce 430',
    '	_GRANT	reduce 430',
    '	_INSERT	reduce 430',
    '	_INTERSECT	reduce 430',
    '	_ORDER	reduce 430',
    '	_REVOKE	reduce 430',
    '	_ROLLBACK	reduce 430',
    '	_SELECT	reduce 430',
    '	_SET	reduce 430',
    '	_TABLE	reduce 430',
    '	_UNION	reduce 430',
    '	_UPDATE	reduce 430',
    '	_VALUES	reduce 430',
    '	_WITH	reduce 430',
    '	.	error',
    '',
    'state 1292:',
    '',
    '	grouping_column_reference_list : grouping_column_reference _	(425)',
    '',
    '	.	reduce 425',
    '',
    'state 1293:',
    '',
    '	group_by_clause : _GROUP _BY grouping_column_reference_list _	(424)',
    '	grouping_column_reference_list : grouping_column_reference_list _ comma grouping_column_reference',
    '',
    '	comma	shift 1380',
    '	$end	reduce 424',
    '	identifier_body	reduce 424',
    '	delimited_identifier	reduce 424',
    '	left_paren	reduce 424',
    '	right_paren	reduce 424',
    '	semicolon	reduce 424',
    '	underscore	reduce 424',
    '	_ALTER	reduce 424',
    '	_COMMIT	reduce 424',
    '	_CONNECT	reduce 424',
    '	_CREATE	reduce 424',
    '	_DECLARE	reduce 424',
    '	_DELETE	reduce 424',
    '	_DISCONNECT	reduce 424',
    '	_DROP	reduce 424',
    '	_EXCEPT	reduce 424',
    '	_FOR	reduce 424',
    '	_GRANT	reduce 424',
    '	_HAVING	reduce 424',
    '	_INSERT	reduce 424',
    '	_INTERSECT	reduce 424',
    '	_ORDER	reduce 424',
    '	_REVOKE	reduce 424',
    '	_ROLLBACK	reduce 424',
    '	_SELECT	reduce 424',
    '	_SET	reduce 424',
    '	_TABLE	reduce 424',
    '	_UNION	reduce 424',
    '	_UPDATE	reduce 424',
    '	_VALUES	reduce 424',
    '	_WITH	reduce 424',
    '	.	error',
    '',
    'state 1294:',
    '',
    '	grouping_column_reference : column_reference _ collate_clause_opt',
    '	collate_clause_opt : _	(98)',
    '',
    '	_COLLATE	shift 414',
    '	$end	reduce 98',
    '	identifier_body	reduce 98',
    '	delimited_identifier	reduce 98',
    '	left_paren	reduce 98',
    '	right_paren	reduce 98',
    '	comma	reduce 98',
    '	semicolon	reduce 98',
    '	underscore	reduce 98',
    '	_ALTER	reduce 98',
    '	_COMMIT	reduce 98',
    '	_CONNECT	reduce 98',
    '	_CREATE	reduce 98',
    '	_DECLARE	reduce 98',
    '	_DELETE	reduce 98',
    '	_DISCONNECT	reduce 98',
    '	_DROP	reduce 98',
    '	_EXCEPT	reduce 98',
    '	_FOR	reduce 98',
    '	_GRANT	reduce 98',
    '	_HAVING	reduce 98',
    '	_INSERT	reduce 98',
    '	_INTERSECT	reduce 98',
    '	_ORDER	reduce 98',
    '	_REVOKE	reduce 98',
    '	_ROLLBACK	reduce 98',
    '	_SELECT	reduce 98',
    '	_SET	reduce 98',
    '	_TABLE	reduce 98',
    '	_UNION	reduce 98',
    '	_UPDATE	reduce 98',
    '	_VALUES	reduce 98',
    '	_WITH	reduce 98',
    '	.	error',
    '',
    '	collate_clause	goto 678',
    '	collate_clause_opt	goto 1381',
    '',
    'state 1295:',
    '',
    '	derived_column_list_opt : left_paren derived_column_list _ right_paren',
    '',
    '	right_paren	shift 1382',
    '	.	error',
    '',
    'state 1296:',
    '',
    '	column_name_list : column_name_list _ comma column_name',
    '	derived_column_list : column_name_list _	(398)',
    '',
    '	comma	shift 786',
    '	right_paren	reduce 398',
    '	.	error',
    '',
    'state 1297:',
    '',
    '	cross_join : table_reference _CROSS _JOIN table_factor _	(404)',
    '',
    '	.	reduce 404',
    '',
    'state 1298:',
    '',
    '	qualified_join : table_reference _FULL outer_opt _JOIN _ table_factor join_specification',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	left_paren	shift 68',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	derived_table	goto 797',
    '	table_factor	goto 1383',
    '	table_subquery	goto 802',
    '	table_name	goto 803',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1299:',
    '',
    '	qualified_join : table_reference _INNER _JOIN table_factor _ join_specification',
    '',
    '	_ON	shift 1303',
    '	_USING	shift 1304',
    '	.	error',
    '',
    '	named_columns_join	goto 1300',
    '	join_condition	goto 1301',
    '	join_specification	goto 1384',
    '',
    'state 1300:',
    '',
    '	join_specification : named_columns_join _	(419)',
    '',
    '	.	reduce 419',
    '',
    'state 1301:',
    '',
    '	join_specification : join_condition _	(418)',
    '',
    '	.	reduce 418',
    '',
    'state 1302:',
    '',
    '	qualified_join : table_reference _JOIN table_factor join_specification _	(405)',
    '',
    '	.	reduce 405',
    '',
    'state 1303:',
    '',
    '	join_condition : _ON _ search_condition',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 636',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXISTS	shift 637',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NOT	shift 638',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UNIQUE	shift 639',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	row_value_constructor_1	goto 617',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 618',
    '	row_value_constructor	goto 619',
    '	overlaps_predicate	goto 620',
    '	match_predicate	goto 621',
    '	unique_predicate	goto 622',
    '	exists_predicate	goto 623',
    '	quantified_comparison_predicate	goto 624',
    '	null_predicate	goto 625',
    '	like_predicate	goto 626',
    '	in_predicate	goto 627',
    '	between_predicate	goto 628',
    '	comparison_predicate	goto 629',
    '	predicate	goto 630',
    '	boolean_primary	goto 631',
    '	boolean_test	goto 632',
    '	boolean_factor	goto 633',
    '	boolean_term	goto 634',
    '	search_condition	goto 1385',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 1304:',
    '',
    '	named_columns_join : _USING _ left_paren join_column_list right_paren',
    '',
    '	left_paren	shift 1386',
    '	.	error',
    '',
    'state 1305:',
    '',
    '	qualified_join : table_reference _LEFT outer_opt _JOIN _ table_factor join_specification',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	left_paren	shift 68',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	derived_table	goto 797',
    '	table_factor	goto 1387',
    '	table_subquery	goto 802',
    '	table_name	goto 803',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1306:',
    '',
    '	qualified_join : table_reference _NATURAL _FULL outer_opt _ _JOIN table_factor',
    '',
    '	_JOIN	shift 1388',
    '	.	error',
    '',
    'state 1307:',
    '',
    '	qualified_join : table_reference _NATURAL _INNER _JOIN _ table_factor',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	left_paren	shift 68',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	derived_table	goto 797',
    '	table_factor	goto 1389',
    '	table_subquery	goto 802',
    '	table_name	goto 803',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1308:',
    '',
    '	qualified_join : table_reference _NATURAL _JOIN table_factor _	(410)',
    '',
    '	.	reduce 410',
    '',
    'state 1309:',
    '',
    '	qualified_join : table_reference _NATURAL _LEFT outer_opt _ _JOIN table_factor',
    '',
    '	_JOIN	shift 1390',
    '	.	error',
    '',
    'state 1310:',
    '',
    '	qualified_join : table_reference _NATURAL _RIGHT outer_opt _ _JOIN table_factor',
    '',
    '	_JOIN	shift 1391',
    '	.	error',
    '',
    'state 1311:',
    '',
    '	qualified_join : table_reference _NATURAL _UNION _JOIN _ table_factor',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	left_paren	shift 68',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	derived_table	goto 797',
    '	table_factor	goto 1392',
    '	table_subquery	goto 802',
    '	table_name	goto 803',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1312:',
    '',
    '	qualified_join : table_reference _RIGHT outer_opt _JOIN _ table_factor join_specification',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	left_paren	shift 68',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	derived_table	goto 797',
    '	table_factor	goto 1393',
    '	table_subquery	goto 802',
    '	table_name	goto 803',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1313:',
    '',
    '	qualified_name_trail_asterisk : identifier period identifier period identifier period _ asterisk',
    '',
    '	asterisk	shift 1394',
    '	.	error',
    '',
    'state 1314:',
    '',
    '	end_field : _SECOND left_paren precision right_paren _	(180)',
    '',
    '	.	reduce 180',
    '',
    'state 1315:',
    '',
    '	like_predicate_escape_opt : _ESCAPE escape_character _	(533)',
    '',
    '	.	reduce 533',
    '',
    'state 1316:',
    '',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	escape_character : expression _	(535)',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	$end	reduce 535',
    '	identifier_body	reduce 535',
    '	delimited_identifier	reduce 535',
    '	left_paren	reduce 535',
    '	right_paren	reduce 535',
    '	comma	reduce 535',
    '	semicolon	reduce 535',
    '	underscore	reduce 535',
    '	_ALTER	reduce 535',
    '	_AND	reduce 535',
    '	_COMMIT	reduce 535',
    '	_CONNECT	reduce 535',
    '	_CREATE	reduce 535',
    '	_CROSS	reduce 535',
    '	_DECLARE	reduce 535',
    '	_DELETE	reduce 535',
    '	_DISCONNECT	reduce 535',
    '	_DROP	reduce 535',
    '	_EXCEPT	reduce 535',
    '	_FOR	reduce 535',
    '	_FULL	reduce 535',
    '	_GRANT	reduce 535',
    '	_GROUP	reduce 535',
    '	_HAVING	reduce 535',
    '	_INNER	reduce 535',
    '	_INSERT	reduce 535',
    '	_INTERSECT	reduce 535',
    '	_IS	reduce 535',
    '	_JOIN	reduce 535',
    '	_LEFT	reduce 535',
    '	_NATURAL	reduce 535',
    '	_OR	reduce 535',
    '	_ORDER	reduce 535',
    '	_REVOKE	reduce 535',
    '	_RIGHT	reduce 535',
    '	_ROLLBACK	reduce 535',
    '	_SELECT	reduce 535',
    '	_SET	reduce 535',
    '	_TABLE	reduce 535',
    '	_THEN	reduce 535',
    '	_UNION	reduce 535',
    '	_UPDATE	reduce 535',
    '	_VALUES	reduce 535',
    '	_WHERE	reduce 535',
    '	_WITH	reduce 535',
    '	.	error',
    '',
    'state 1317:',
    '',
    '	like_predicate : expression _NOT _LIKE pattern like_predicate_escape_opt _	(531)',
    '',
    '	.	reduce 531',
    '',
    'state 1318:',
    '',
    '	between_predicate : row_value_constructor _BETWEEN row_value_constructor _AND row_value_constructor _	(522)',
    '',
    '	.	reduce 522',
    '',
    'state 1319:',
    '',
    '	in_predicate_value : left_paren in_value_list right_paren _	(527)',
    '',
    '	.	reduce 527',
    '',
    'state 1320:',
    '',
    '	in_value_list : in_value_list comma _ expression',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 1395',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 1321:',
    '',
    '	match_predicate : row_value_constructor _MATCH unique_opt partial_full_opt table_subquery _	(546)',
    '',
    '	.	reduce 546',
    '',
    'state 1322:',
    '',
    '	between_predicate : row_value_constructor _NOT _BETWEEN row_value_constructor _AND _ row_value_constructor',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 248',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 238',
    '	row_value_constructor	goto 1396',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 1323:',
    '',
    '	character_bit_substring_function : _SUBSTRING left_paren expression _FROM start_position for_strlength_opt right_paren _	(472)',
    '',
    '	.	reduce 472',
    '',
    'state 1324:',
    '',
    '	for_strlength_opt : _FOR string_length _	(474)',
    '',
    '	.	reduce 474',
    '',
    'state 1325:',
    '',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '	string_length : expression _	(476)',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	right_paren	reduce 476',
    '	.	error',
    '',
    'state 1326:',
    '',
    '	declare_cursor : _DECLARE cursor_name insensitive_opt scroll_opt _CURSOR _FOR _ cursor_specification',
    '	dynamic_declare_cursor : _DECLARE cursor_name insensitive_opt scroll_opt _CURSOR _FOR _ statement_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	left_paren	shift 68',
    '	underscore	shift 69',
    '	_SELECT	shift 83',
    '	_TABLE	shift 85',
    '	_VALUES	shift 87',
    '	.	error',
    '',
    '	statement_name	goto 1397',
    '	cursor_specification	goto 1398',
    '	explicit_table	goto 49',
    '	table_value_constructor	goto 50',
    '	query_specification	goto 51',
    '	table_subquery	goto 52',
    '	simple_table	goto 53',
    '	non_join_query_primary	goto 54',
    '	query_term	goto 55',
    '	non_join_query_term	goto 56',
    '	query_expression	goto 1399',
    '	actual_identifier	goto 61',
    '	identifier	goto 1400',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1327:',
    '',
    '	procedure : _PROCEDURE procedure_name parameter_declaration_list semicolon SQL_procedure_statement semicolon _	(604)',
    '',
    '	.	reduce 604',
    '',
    'state 1328:',
    '',
    '	close_statement : _CLOSE cursor_name _	(799)',
    '',
    '	.	reduce 799',
    '',
    'state 1329:',
    '',
    '	delete_statement__positioned : _DELETE _FROM _ table_name _WHERE _CURRENT _OF cursor_name',
    '	delete_statement__searched : _DELETE _FROM _ table_name where_clause_opt',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	table_name	goto 1401',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1330:',
    '',
    '	fetch_orientation_opt : fetch_orientation _ _FROM',
    '',
    '	_FROM	shift 1402',
    '	.	error',
    '',
    'state 1331:',
    '',
    '	fetch_statement : _FETCH fetch_orientation_opt _ cursor_name _INTO fetch_target_list',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	cursor_name	goto 1403',
    '	actual_identifier	goto 61',
    '	identifier	goto 685',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1332:',
    '',
    '	fetch_orientation : _ABSOLUTE _ simple_value_specification',
    '',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	digit	shift 147',
    '	plus_sign	shift 148',
    '	minus_sign	shift 149',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_DATE	shift 154',
    '	_INTERVAL	shift 156',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	.	error',
    '',
    '	simple_value_specification	goto 1404',
    '	parameter_name	goto 122',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 128',
    '	signed_numeric_literal	goto 129',
    '	literal	goto 130',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 132',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	sign	goto 137',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 142',
    '',
    'state 1333:',
    '',
    '	fetch_orientation : _FIRST _	(790)',
    '',
    '	.	reduce 790',
    '',
    'state 1334:',
    '',
    '	fetch_orientation_opt : _FROM _	(786)',
    '',
    '	.	reduce 786',
    '',
    'state 1335:',
    '',
    '	fetch_orientation : _LAST _	(791)',
    '',
    '	.	reduce 791',
    '',
    'state 1336:',
    '',
    '	fetch_orientation : _NEXT _	(788)',
    '',
    '	.	reduce 788',
    '',
    'state 1337:',
    '',
    '	fetch_orientation : _PRIOR _	(789)',
    '',
    '	.	reduce 789',
    '',
    'state 1338:',
    '',
    '	fetch_orientation : _RELATIVE _ simple_value_specification',
    '',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	digit	shift 147',
    '	plus_sign	shift 148',
    '	minus_sign	shift 149',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_DATE	shift 154',
    '	_INTERVAL	shift 156',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	.	error',
    '',
    '	simple_value_specification	goto 1405',
    '	parameter_name	goto 122',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 128',
    '	signed_numeric_literal	goto 129',
    '	literal	goto 130',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 132',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	sign	goto 137',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 142',
    '',
    'state 1339:',
    '',
    '	open_statement : _OPEN cursor_name _	(783)',
    '',
    '	.	reduce 783',
    '',
    'state 1340:',
    '',
    '	select_statement__single_row : _SELECT set_quantifier_opt _ select_list _INTO select_target_list table_expression',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 374',
    '	asterisk	shift 375',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	derived_column	goto 367',
    '	select_sublist	goto 368',
    '	select_list_opt	goto 369',
    '	select_list	goto 1406',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 371',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name_trail_asterisk	goto 372',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 373',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 1341:',
    '',
    '	update_statement__positioned : _UPDATE table_name _ _SET set_clause_list _WHERE _CURRENT _OF cursor_name',
    '	update_statement__searched : _UPDATE table_name _ _SET set_clause_list where_clause_opt',
    '',
    '	_SET	shift 1407',
    '	.	error',
    '',
    'state 1342:',
    '',
    '	parameter_declarations : parameter_declarations comma parameter_declaration _	(608)',
    '',
    '	.	reduce 608',
    '',
    'state 1343:',
    '',
    '	check_constraint_definition : _CHECK left_paren search_condition right_paren _	(265)',
    '',
    '	.	reduce 265',
    '',
    'state 1344:',
    '',
    '	column_constraint_definition : constraint_name_definition_opt _ column_constraint constraint_attributes_opt',
    '',
    '	_CHECK	shift 897',
    '	_NOT	shift 1412',
    '	_PRIMARY	shift 916',
    '	_REFERENCES	shift 1413',
    '	_UNIQUE	shift 917',
    '	.	error',
    '',
    '	check_constraint_definition	goto 1408',
    '	references_specification	goto 1409',
    '	unique_specification	goto 1410',
    '	column_constraint	goto 1411',
    '',
    'state 1345:',
    '',
    '	column_constraint_definition_opt : column_constraint_definition _	(97)',
    '',
    '	.	reduce 97',
    '',
    'state 1346:',
    '',
    '	column_definition : column_name column_definition_sel default_clause_opt column_constraint_definition_opt _ collate_clause_opt',
    '	collate_clause_opt : _	(98)',
    '',
    '	_COLLATE	shift 414',
    '	$end	reduce 98',
    '	identifier_body	reduce 98',
    '	delimited_identifier	reduce 98',
    '	left_paren	reduce 98',
    '	right_paren	reduce 98',
    '	comma	reduce 98',
    '	semicolon	reduce 98',
    '	underscore	reduce 98',
    '	_ALTER	reduce 98',
    '	_COMMIT	reduce 98',
    '	_CONNECT	reduce 98',
    '	_CREATE	reduce 98',
    '	_DECLARE	reduce 98',
    '	_DELETE	reduce 98',
    '	_DISCONNECT	reduce 98',
    '	_DROP	reduce 98',
    '	_GRANT	reduce 98',
    '	_INSERT	reduce 98',
    '	_REVOKE	reduce 98',
    '	_ROLLBACK	reduce 98',
    '	_SELECT	reduce 98',
    '	_SET	reduce 98',
    '	_TABLE	reduce 98',
    '	_UPDATE	reduce 98',
    '	_VALUES	reduce 98',
    '	.	error',
    '',
    '	collate_clause	goto 678',
    '	collate_clause_opt	goto 1414',
    '',
    'state 1347:',
    '',
    '	unique_constraint_definition : unique_specification left_paren unique_column_list right_paren _	(573)',
    '',
    '	.	reduce 573',
    '',
    'state 1348:',
    '',
    '	referential_constraint_definition : _FOREIGN _KEY left_paren referencing_columns _ right_paren references_specification',
    '',
    '	right_paren	shift 1415',
    '	.	error',
    '',
    'state 1349:',
    '',
    '	reference_column_list : column_name_list _	(248)',
    '	column_name_list : column_name_list _ comma column_name',
    '',
    '	comma	shift 786',
    '	right_paren	reduce 248',
    '	.	error',
    '',
    'state 1350:',
    '',
    '	referencing_columns : reference_column_list _	(576)',
    '',
    '	.	reduce 576',
    '',
    'state 1351:',
    '',
    '	table_commit_opts : _ON _COMMIT _DELETE _ROWS _	(656)',
    '',
    '	.	reduce 656',
    '',
    'state 1352:',
    '',
    '	table_commit_opts : _ON _COMMIT _PRESERVE _ROWS _	(657)',
    '',
    '	.	reduce 657',
    '',
    'state 1353:',
    '',
    '	table_element_list_opt : table_element_list_opt comma table_element _	(88)',
    '',
    '	.	reduce 88',
    '',
    'state 1354:',
    '',
    '	limited_collation_definition : _COLLATION _FROM collation_source _	(701)',
    '',
    '	.	reduce 701',
    '',
    'state 1355:',
    '',
    '	pad_attribute_opt : _NO _PAD _	(716)',
    '',
    '	.	reduce 716',
    '',
    'state 1356:',
    '',
    '	pad_attribute_opt : _PAD _SPACE _	(717)',
    '',
    '	.	reduce 717',
    '',
    'state 1357:',
    '',
    '	collating_sequence_definition : _DESC left_paren collation_name _ right_paren',
    '',
    '	right_paren	shift 1416',
    '	.	error',
    '',
    'state 1358:',
    '',
    '	external_collation : _EXTERNAL left_paren quote _ external_collation_name quote right_paren',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	external_collation_name	goto 1417',
    '	collation_name	goto 1418',
    '	qualified_name	goto 313',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1359:',
    '',
    '	translation_collation : _TRANSLATION translation_name translation_collation_opt _	(711)',
    '',
    '	.	reduce 711',
    '',
    'state 1360:',
    '',
    '	translation_collation_opt : _THEN _ _COLLATION collation_name',
    '',
    '	_COLLATION	shift 1419',
    '	.	error',
    '',
    'state 1361:',
    '',
    '	numeric_precision_scale_opt : left_paren precision comma scale _ right_paren',
    '',
    '	right_paren	shift 1420',
    '	.	error',
    '',
    'state 1362:',
    '',
    '	unsigned_integer : unsigned_integer _ digit',
    '	scale : unsigned_integer _	(149)',
    '',
    '	digit	shift 331',
    '	right_paren	reduce 149',
    '	.	error',
    '',
    'state 1363:',
    '',
    '	tz_opt : _WITH _TIME _ZONE _	(162)',
    '',
    '	.	reduce 162',
    '',
    'state 1364:',
    '',
    '	translation_specification : schema_translation_name _	(724)',
    '',
    '	.	reduce 724',
    '',
    'state 1365:',
    '',
    '	translation_specification : external_translation _	(722)',
    '',
    '	.	reduce 722',
    '',
    'state 1366:',
    '',
    '	translation_source : translation_specification _	(721)',
    '',
    '	.	reduce 721',
    '',
    'state 1367:',
    '',
    '	translation_definition : _CREATE _TRANSLATION translation_name _FOR source_character_set_specification _TO target_character_set_specification _FROM translation_source _	(718)',
    '',
    '	.	reduce 718',
    '',
    'state 1368:',
    '',
    '	schema_translation_name : translation_name _	(727)',
    '',
    '	.	reduce 727',
    '',
    'state 1369:',
    '',
    '	external_translation : _EXTERNAL _ left_paren quote external_translation_name quote right_paren',
    '',
    '	left_paren	shift 1421',
    '	.	error',
    '',
    'state 1370:',
    '',
    '	translation_specification : _IDENTITY _	(723)',
    '',
    '	.	reduce 723',
    '',
    'state 1371:',
    '',
    '	view_check_opt : _WITH _CASCADED _CHECK _ _OPTION',
    '',
    '	_OPTION	shift 1422',
    '	.	error',
    '',
    'state 1372:',
    '',
    '	view_check_opt : _WITH _CHECK _OPTION _	(662)',
    '',
    '	.	reduce 662',
    '',
    'state 1373:',
    '',
    '	view_check_opt : _WITH _LOCAL _CHECK _ _OPTION',
    '',
    '	_OPTION	shift 1423',
    '	.	error',
    '',
    'state 1374:',
    '',
    '	temporary_table_declaration_opt : _ON _COMMIT _DELETE _ _ROWS',
    '',
    '	_ROWS	shift 1424',
    '	.	error',
    '',
    'state 1375:',
    '',
    '	temporary_table_declaration_opt : _ON _COMMIT _PRESERVE _ _ROWS',
    '',
    '	_ROWS	shift 1425',
    '	.	error',
    '',
    'state 1376:',
    '',
    '	interval_string_literal : unsigned_integer space unsigned_integer colon unsigned_integer colon _ seconds_value',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	seconds_value	goto 1426',
    '	unsigned_integer	goto 1282',
    '',
    'state 1377:',
    '',
    '	time_string : quote time_value quote quote time_value time_zone_interval quote _	(45)',
    '',
    '	.	reduce 45',
    '',
    'state 1378:',
    '',
    '	time_zone_interval : sign unsigned_integer colon _ unsigned_integer',
    '',
    '	digit	shift 147',
    '	.	error',
    '',
    '	unsigned_integer	goto 1427',
    '',
    'state 1379:',
    '',
    '	grant_option : _WITH _GRANT _OPTION _	(670)',
    '',
    '	.	reduce 670',
    '',
    'state 1380:',
    '',
    '	grouping_column_reference_list : grouping_column_reference_list comma _ grouping_column_reference',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	grouping_column_reference	goto 1428',
    '	column_reference	goto 1294',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1381:',
    '',
    '	grouping_column_reference : column_reference collate_clause_opt _	(427)',
    '',
    '	.	reduce 427',
    '',
    'state 1382:',
    '',
    '	derived_column_list_opt : left_paren derived_column_list right_paren _	(397)',
    '',
    '	.	reduce 397',
    '',
    'state 1383:',
    '',
    '	qualified_join : table_reference _FULL outer_opt _JOIN table_factor _ join_specification',
    '',
    '	_ON	shift 1303',
    '	_USING	shift 1304',
    '	.	error',
    '',
    '	named_columns_join	goto 1300',
    '	join_condition	goto 1301',
    '	join_specification	goto 1429',
    '',
    'state 1384:',
    '',
    '	qualified_join : table_reference _INNER _JOIN table_factor join_specification _	(406)',
    '',
    '	.	reduce 406',
    '',
    'state 1385:',
    '',
    '	join_condition : _ON search_condition _	(420)',
    '	search_condition : search_condition _ _OR boolean_term',
    '',
    '	_OR	shift 857',
    '	$end	reduce 420',
    '	identifier_body	reduce 420',
    '	delimited_identifier	reduce 420',
    '	left_paren	reduce 420',
    '	right_paren	reduce 420',
    '	comma	reduce 420',
    '	semicolon	reduce 420',
    '	underscore	reduce 420',
    '	_ALTER	reduce 420',
    '	_COMMIT	reduce 420',
    '	_CONNECT	reduce 420',
    '	_CREATE	reduce 420',
    '	_CROSS	reduce 420',
    '	_DECLARE	reduce 420',
    '	_DELETE	reduce 420',
    '	_DISCONNECT	reduce 420',
    '	_DROP	reduce 420',
    '	_EXCEPT	reduce 420',
    '	_FOR	reduce 420',
    '	_FULL	reduce 420',
    '	_GRANT	reduce 420',
    '	_GROUP	reduce 420',
    '	_HAVING	reduce 420',
    '	_INNER	reduce 420',
    '	_INSERT	reduce 420',
    '	_INTERSECT	reduce 420',
    '	_JOIN	reduce 420',
    '	_LEFT	reduce 420',
    '	_NATURAL	reduce 420',
    '	_ORDER	reduce 420',
    '	_REVOKE	reduce 420',
    '	_RIGHT	reduce 420',
    '	_ROLLBACK	reduce 420',
    '	_SELECT	reduce 420',
    '	_SET	reduce 420',
    '	_TABLE	reduce 420',
    '	_UNION	reduce 420',
    '	_UPDATE	reduce 420',
    '	_VALUES	reduce 420',
    '	_WHERE	reduce 420',
    '	_WITH	reduce 420',
    '	.	error',
    '',
    'state 1386:',
    '',
    '	named_columns_join : _USING left_paren _ join_column_list right_paren',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	join_column_list	goto 1430',
    '	column_name_list	goto 1431',
    '	column_name	goto 551',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1387:',
    '',
    '	qualified_join : table_reference _LEFT outer_opt _JOIN table_factor _ join_specification',
    '',
    '	_ON	shift 1303',
    '	_USING	shift 1304',
    '	.	error',
    '',
    '	named_columns_join	goto 1300',
    '	join_condition	goto 1301',
    '	join_specification	goto 1432',
    '',
    'state 1388:',
    '',
    '	qualified_join : table_reference _NATURAL _FULL outer_opt _JOIN _ table_factor',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	left_paren	shift 68',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	derived_table	goto 797',
    '	table_factor	goto 1433',
    '	table_subquery	goto 802',
    '	table_name	goto 803',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1389:',
    '',
    '	qualified_join : table_reference _NATURAL _INNER _JOIN table_factor _	(411)',
    '',
    '	.	reduce 411',
    '',
    'state 1390:',
    '',
    '	qualified_join : table_reference _NATURAL _LEFT outer_opt _JOIN _ table_factor',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	left_paren	shift 68',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	derived_table	goto 797',
    '	table_factor	goto 1434',
    '	table_subquery	goto 802',
    '	table_name	goto 803',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1391:',
    '',
    '	qualified_join : table_reference _NATURAL _RIGHT outer_opt _JOIN _ table_factor',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	left_paren	shift 68',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	derived_table	goto 797',
    '	table_factor	goto 1435',
    '	table_subquery	goto 802',
    '	table_name	goto 803',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1392:',
    '',
    '	qualified_join : table_reference _NATURAL _UNION _JOIN table_factor _	(415)',
    '',
    '	.	reduce 415',
    '',
    'state 1393:',
    '',
    '	qualified_join : table_reference _RIGHT outer_opt _JOIN table_factor _ join_specification',
    '',
    '	_ON	shift 1303',
    '	_USING	shift 1304',
    '	.	error',
    '',
    '	named_columns_join	goto 1300',
    '	join_condition	goto 1301',
    '	join_specification	goto 1436',
    '',
    'state 1394:',
    '',
    '	qualified_name_trail_asterisk : identifier period identifier period identifier period asterisk _	(192)',
    '',
    '	.	reduce 192',
    '',
    'state 1395:',
    '',
    '	in_value_list : in_value_list comma expression _	(529)',
    '	expression : expression _ plus_sign multiplicative_expression',
    '	expression : expression _ minus_sign multiplicative_expression',
    '	expression : expression _ concatenation_operator multiplicative_expression',
    '',
    '	concatenation_operator	shift 421',
    '	plus_sign	shift 422',
    '	minus_sign	shift 423',
    '	right_paren	reduce 529',
    '	comma	reduce 529',
    '	.	error',
    '',
    'state 1396:',
    '',
    '	between_predicate : row_value_constructor _NOT _BETWEEN row_value_constructor _AND row_value_constructor _	(523)',
    '',
    '	.	reduce 523',
    '',
    'state 1397:',
    '',
    '	dynamic_declare_cursor : _DECLARE cursor_name insensitive_opt scroll_opt _CURSOR _FOR statement_name _	(602)',
    '',
    '	.	reduce 602',
    '',
    'state 1398:',
    '',
    '	declare_cursor : _DECLARE cursor_name insensitive_opt scroll_opt _CURSOR _FOR cursor_specification _	(580)',
    '',
    '	.	reduce 580',
    '',
    'state 1399:',
    '',
    '	query_expression : query_expression _ _UNION all_opt corresponding_spec_opt query_term',
    '	query_expression : query_expression _ _EXCEPT all_opt corresponding_spec_opt query_term',
    '	cursor_specification : query_expression _ order_by_clause_opt updatability_clause_opt',
    '	order_by_clause_opt : _	(587)',
    '',
    '	_EXCEPT	shift 91',
    '	_ORDER	shift 92',
    '	_UNION	shift 93',
    '	$end	reduce 587',
    '	_FOR	reduce 587',
    '	.	error',
    '',
    '	order_by_clause_opt	goto 1437',
    '',
    'state 1400:',
    '',
    '	statement_name : identifier _	(603)',
    '',
    '	.	reduce 603',
    '',
    'state 1401:',
    '',
    '	delete_statement__positioned : _DELETE _FROM table_name _ _WHERE _CURRENT _OF cursor_name',
    '	delete_statement__searched : _DELETE _FROM table_name _ where_clause_opt',
    '	where_clause_opt : _	(377)',
    '',
    '	_WHERE	shift 1438',
    '	semicolon	reduce 377',
    '	.	error',
    '',
    '	where_clause	goto 513',
    '	where_clause_opt	goto 514',
    '',
    'state 1402:',
    '',
    '	fetch_orientation_opt : fetch_orientation _FROM _	(787)',
    '',
    '	.	reduce 787',
    '',
    'state 1403:',
    '',
    '	fetch_statement : _FETCH fetch_orientation_opt cursor_name _ _INTO fetch_target_list',
    '',
    '	_INTO	shift 1439',
    '	.	error',
    '',
    'state 1404:',
    '',
    '	fetch_orientation : _ABSOLUTE simple_value_specification _	(792)',
    '',
    '	.	reduce 792',
    '',
    'state 1405:',
    '',
    '	fetch_orientation : _RELATIVE simple_value_specification _	(793)',
    '',
    '	.	reduce 793',
    '',
    'state 1406:',
    '',
    '	select_statement__single_row : _SELECT set_quantifier_opt select_list _ _INTO select_target_list table_expression',
    '',
    '	_INTO	shift 1440',
    '	.	error',
    '',
    'state 1407:',
    '',
    '	update_statement__positioned : _UPDATE table_name _SET _ set_clause_list _WHERE _CURRENT _OF cursor_name',
    '	update_statement__searched : _UPDATE table_name _SET _ set_clause_list where_clause_opt',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	object_column	goto 583',
    '	set_clause	goto 584',
    '	set_clause_list	goto 1441',
    '	column_name	goto 586',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1408:',
    '',
    '	column_constraint : check_constraint_definition _	(235)',
    '',
    '	.	reduce 235',
    '',
    'state 1409:',
    '',
    '	column_constraint : references_specification _	(234)',
    '',
    '	.	reduce 234',
    '',
    'state 1410:',
    '',
    '	column_constraint : unique_specification _	(233)',
    '',
    '	.	reduce 233',
    '',
    'state 1411:',
    '',
    '	column_constraint_definition : constraint_name_definition_opt column_constraint _ constraint_attributes_opt',
    '	constraint_attributes_opt : _	(558)',
    '',
    '	_DEFERRABLE	shift 713',
    '	_INITIALLY	shift 714',
    '	$end	reduce 558',
    '	identifier_body	reduce 558',
    '	delimited_identifier	reduce 558',
    '	left_paren	reduce 558',
    '	right_paren	reduce 558',
    '	comma	reduce 558',
    '	semicolon	reduce 558',
    '	underscore	reduce 558',
    '	_ALTER	reduce 558',
    '	_COLLATE	reduce 558',
    '	_COMMIT	reduce 558',
    '	_CONNECT	reduce 558',
    '	_CREATE	reduce 558',
    '	_DECLARE	reduce 558',
    '	_DELETE	reduce 558',
    '	_DISCONNECT	reduce 558',
    '	_DROP	reduce 558',
    '	_GRANT	reduce 558',
    '	_INSERT	reduce 558',
    '	_REVOKE	reduce 558',
    '	_ROLLBACK	reduce 558',
    '	_SELECT	reduce 558',
    '	_SET	reduce 558',
    '	_TABLE	reduce 558',
    '	_UPDATE	reduce 558',
    '	_VALUES	reduce 558',
    '	.	error',
    '',
    '	constraint_check_time	goto 710',
    '	constraint_attributes	goto 711',
    '	constraint_attributes_opt	goto 1442',
    '',
    'state 1412:',
    '',
    '	column_constraint : _NOT _ _NULL',
    '',
    '	_NULL	shift 1443',
    '	.	error',
    '',
    'state 1413:',
    '',
    '	references_specification : _REFERENCES _ referenced_table_and_columns match_type_opt referential_triggered_action_opt',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	_MODULE	shift 198',
    '	.	error',
    '',
    '	table_name	goto 1444',
    '	referenced_table_and_columns	goto 1445',
    '	qualified_name	goto 195',
    '	qualified_local_table_name	goto 196',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1414:',
    '',
    '	column_definition : column_name column_definition_sel default_clause_opt column_constraint_definition_opt collate_clause_opt _	(91)',
    '',
    '	.	reduce 91',
    '',
    'state 1415:',
    '',
    '	referential_constraint_definition : _FOREIGN _KEY left_paren referencing_columns right_paren _ references_specification',
    '',
    '	_REFERENCES	shift 1413',
    '	.	error',
    '',
    '	references_specification	goto 1446',
    '',
    'state 1416:',
    '',
    '	collating_sequence_definition : _DESC left_paren collation_name right_paren _	(706)',
    '',
    '	.	reduce 706',
    '',
    'state 1417:',
    '',
    '	external_collation : _EXTERNAL left_paren quote external_collation_name _ quote right_paren',
    '',
    '	quote	shift 1447',
    '	.	error',
    '',
    'state 1418:',
    '',
    '	external_collation_name : collation_name _	(709)',
    '',
    '	.	reduce 709',
    '',
    'state 1419:',
    '',
    '	translation_collation_opt : _THEN _COLLATION _ collation_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	collation_name	goto 1448',
    '	qualified_name	goto 313',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1420:',
    '',
    '	numeric_precision_scale_opt : left_paren precision comma scale right_paren _	(146)',
    '',
    '	.	reduce 146',
    '',
    'state 1421:',
    '',
    '	external_translation : _EXTERNAL left_paren _ quote external_translation_name quote right_paren',
    '',
    '	quote	shift 1449',
    '	.	error',
    '',
    'state 1422:',
    '',
    '	view_check_opt : _WITH _CASCADED _CHECK _OPTION _	(663)',
    '',
    '	.	reduce 663',
    '',
    'state 1423:',
    '',
    '	view_check_opt : _WITH _LOCAL _CHECK _OPTION _	(664)',
    '',
    '	.	reduce 664',
    '',
    'state 1424:',
    '',
    '	temporary_table_declaration_opt : _ON _COMMIT _DELETE _ROWS _	(83)',
    '',
    '	.	reduce 83',
    '',
    'state 1425:',
    '',
    '	temporary_table_declaration_opt : _ON _COMMIT _PRESERVE _ROWS _	(82)',
    '',
    '	.	reduce 82',
    '',
    'state 1426:',
    '',
    '	interval_string_literal : unsigned_integer space unsigned_integer colon unsigned_integer colon seconds_value _	(57)',
    '',
    '	.	reduce 57',
    '',
    'state 1427:',
    '',
    '	time_zone_interval : sign unsigned_integer colon unsigned_integer _	(49)',
    '	unsigned_integer : unsigned_integer _ digit',
    '',
    '	digit	shift 331',
    '	quote	reduce 49',
    '	.	error',
    '',
    'state 1428:',
    '',
    '	grouping_column_reference_list : grouping_column_reference_list comma grouping_column_reference _	(426)',
    '',
    '	.	reduce 426',
    '',
    'state 1429:',
    '',
    '	qualified_join : table_reference _FULL outer_opt _JOIN table_factor join_specification _	(409)',
    '',
    '	.	reduce 409',
    '',
    'state 1430:',
    '',
    '	named_columns_join : _USING left_paren join_column_list _ right_paren',
    '',
    '	right_paren	shift 1450',
    '	.	error',
    '',
    'state 1431:',
    '',
    '	column_name_list : column_name_list _ comma column_name',
    '	join_column_list : column_name_list _	(422)',
    '',
    '	comma	shift 786',
    '	right_paren	reduce 422',
    '	.	error',
    '',
    'state 1432:',
    '',
    '	qualified_join : table_reference _LEFT outer_opt _JOIN table_factor join_specification _	(407)',
    '',
    '	.	reduce 407',
    '',
    'state 1433:',
    '',
    '	qualified_join : table_reference _NATURAL _FULL outer_opt _JOIN table_factor _	(414)',
    '',
    '	.	reduce 414',
    '',
    'state 1434:',
    '',
    '	qualified_join : table_reference _NATURAL _LEFT outer_opt _JOIN table_factor _	(412)',
    '',
    '	.	reduce 412',
    '',
    'state 1435:',
    '',
    '	qualified_join : table_reference _NATURAL _RIGHT outer_opt _JOIN table_factor _	(413)',
    '',
    '	.	reduce 413',
    '',
    'state 1436:',
    '',
    '	qualified_join : table_reference _RIGHT outer_opt _JOIN table_factor join_specification _	(408)',
    '',
    '	.	reduce 408',
    '',
    'state 1437:',
    '',
    '	cursor_specification : query_expression order_by_clause_opt _ updatability_clause_opt',
    '	updatability_clause_opt : _	(597)',
    '',
    '	_FOR	shift 1452',
    '	$end	reduce 597',
    '	.	error',
    '',
    '	updatability_clause_opt	goto 1451',
    '',
    'state 1438:',
    '',
    '	delete_statement__positioned : _DELETE _FROM table_name _WHERE _ _CURRENT _OF cursor_name',
    '	where_clause : _WHERE _ search_condition',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 636',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT	shift 1453',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXISTS	shift 637',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NOT	shift 638',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UNIQUE	shift 639',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	row_value_constructor_1	goto 617',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 618',
    '	row_value_constructor	goto 619',
    '	overlaps_predicate	goto 620',
    '	match_predicate	goto 621',
    '	unique_predicate	goto 622',
    '	exists_predicate	goto 623',
    '	quantified_comparison_predicate	goto 624',
    '	null_predicate	goto 625',
    '	like_predicate	goto 626',
    '	in_predicate	goto 627',
    '	between_predicate	goto 628',
    '	comparison_predicate	goto 629',
    '	predicate	goto 630',
    '	boolean_primary	goto 631',
    '	boolean_test	goto 632',
    '	boolean_factor	goto 633',
    '	boolean_term	goto 634',
    '	search_condition	goto 766',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 1439:',
    '',
    '	fetch_statement : _FETCH fetch_orientation_opt cursor_name _INTO _ fetch_target_list',
    '',
    '	colon	shift 151',
    '	.	error',
    '',
    '	target_specification	goto 1454',
    '	fetch_target_list	goto 1455',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 1456',
    '',
    'state 1440:',
    '',
    '	select_statement__single_row : _SELECT set_quantifier_opt select_list _INTO _ select_target_list table_expression',
    '',
    '	colon	shift 151',
    '	.	error',
    '',
    '	select_target_list	goto 1457',
    '	target_specification	goto 1458',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 1456',
    '',
    'state 1441:',
    '',
    '	update_statement__positioned : _UPDATE table_name _SET set_clause_list _ _WHERE _CURRENT _OF cursor_name',
    '	update_statement__searched : _UPDATE table_name _SET set_clause_list _ where_clause_opt',
    '	set_clause_list : set_clause_list _ comma set_clause',
    '	where_clause_opt : _	(377)',
    '',
    '	comma	shift 820',
    '	_WHERE	shift 1459',
    '	semicolon	reduce 377',
    '	.	error',
    '',
    '	where_clause	goto 513',
    '	where_clause_opt	goto 819',
    '',
    'state 1442:',
    '',
    '	column_constraint_definition : constraint_name_definition_opt column_constraint constraint_attributes_opt _	(227)',
    '',
    '	.	reduce 227',
    '',
    'state 1443:',
    '',
    '	column_constraint : _NOT _NULL _	(232)',
    '',
    '	.	reduce 232',
    '',
    'state 1444:',
    '',
    '	*** conflicts:',
    '',
    '	shift 1461, reduce 244 on left_paren',
    '',
    '	referenced_table_and_columns : table_name _ reference_column_list_opt',
    '	reference_column_list_opt : _	(244)',
    '',
    '	left_paren	shift 1461',
    '	$end	reduce 244',
    '	identifier_body	reduce 244',
    '	delimited_identifier	reduce 244',
    '	right_paren	reduce 244',
    '	comma	reduce 244',
    '	semicolon	reduce 244',
    '	underscore	reduce 244',
    '	_ALTER	reduce 244',
    '	_COLLATE	reduce 244',
    '	_COMMIT	reduce 244',
    '	_CONNECT	reduce 244',
    '	_CREATE	reduce 244',
    '	_DECLARE	reduce 244',
    '	_DEFERRABLE	reduce 244',
    '	_DELETE	reduce 244',
    '	_DISCONNECT	reduce 244',
    '	_DROP	reduce 244',
    '	_GRANT	reduce 244',
    '	_INITIALLY	reduce 244',
    '	_INSERT	reduce 244',
    '	_MATCH	reduce 244',
    '	_ON	reduce 244',
    '	_REVOKE	reduce 244',
    '	_ROLLBACK	reduce 244',
    '	_SELECT	reduce 244',
    '	_SET	reduce 244',
    '	_TABLE	reduce 244',
    '	_UPDATE	reduce 244',
    '	_VALUES	reduce 244',
    '	.	error',
    '',
    '	reference_column_list_opt	goto 1460',
    '',
    'state 1445:',
    '',
    '	references_specification : _REFERENCES referenced_table_and_columns _ match_type_opt referential_triggered_action_opt',
    '	match_type_opt : _	(239)',
    '',
    '	_MATCH	shift 1463',
    '	$end	reduce 239',
    '	identifier_body	reduce 239',
    '	delimited_identifier	reduce 239',
    '	left_paren	reduce 239',
    '	right_paren	reduce 239',
    '	comma	reduce 239',
    '	semicolon	reduce 239',
    '	underscore	reduce 239',
    '	_ALTER	reduce 239',
    '	_COLLATE	reduce 239',
    '	_COMMIT	reduce 239',
    '	_CONNECT	reduce 239',
    '	_CREATE	reduce 239',
    '	_DECLARE	reduce 239',
    '	_DEFERRABLE	reduce 239',
    '	_DELETE	reduce 239',
    '	_DISCONNECT	reduce 239',
    '	_DROP	reduce 239',
    '	_GRANT	reduce 239',
    '	_INITIALLY	reduce 239',
    '	_INSERT	reduce 239',
    '	_ON	reduce 239',
    '	_REVOKE	reduce 239',
    '	_ROLLBACK	reduce 239',
    '	_SELECT	reduce 239',
    '	_SET	reduce 239',
    '	_TABLE	reduce 239',
    '	_UPDATE	reduce 239',
    '	_VALUES	reduce 239',
    '	.	error',
    '',
    '	match_type_opt	goto 1462',
    '',
    'state 1446:',
    '',
    '	referential_constraint_definition : _FOREIGN _KEY left_paren referencing_columns right_paren references_specification _	(575)',
    '',
    '	.	reduce 575',
    '',
    'state 1447:',
    '',
    '	external_collation : _EXTERNAL left_paren quote external_collation_name quote _ right_paren',
    '',
    '	right_paren	shift 1464',
    '	.	error',
    '',
    'state 1448:',
    '',
    '	translation_collation_opt : _THEN _COLLATION collation_name _	(713)',
    '',
    '	.	reduce 713',
    '',
    'state 1449:',
    '',
    '	external_translation : _EXTERNAL left_paren quote _ external_translation_name quote right_paren',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	external_translation_name	goto 1465',
    '	translation_name	goto 1466',
    '	qualified_name	goto 322',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1450:',
    '',
    '	named_columns_join : _USING left_paren join_column_list right_paren _	(421)',
    '',
    '	.	reduce 421',
    '',
    'state 1451:',
    '',
    '	cursor_specification : query_expression order_by_clause_opt updatability_clause_opt _	(586)',
    '',
    '	.	reduce 586',
    '',
    'state 1452:',
    '',
    '	updatability_clause_opt : _FOR _ _READ _ONLY',
    '	updatability_clause_opt : _FOR _ _UPDATE updatability_column_opt',
    '',
    '	_READ	shift 1467',
    '	_UPDATE	shift 1468',
    '	.	error',
    '',
    'state 1453:',
    '',
    '	delete_statement__positioned : _DELETE _FROM table_name _WHERE _CURRENT _ _OF cursor_name',
    '',
    '	_OF	shift 1469',
    '	.	error',
    '',
    'state 1454:',
    '',
    '	fetch_target_list : target_specification _	(796)',
    '',
    '	.	reduce 796',
    '',
    'state 1455:',
    '',
    '	fetch_statement : _FETCH fetch_orientation_opt cursor_name _INTO fetch_target_list _	(784)',
    '	fetch_target_list : fetch_target_list _ comma target_specification',
    '',
    '	comma	shift 1470',
    '	semicolon	reduce 784',
    '	.	error',
    '',
    'state 1456:',
    '',
    '	target_specification : parameter_specification _	(798)',
    '',
    '	.	reduce 798',
    '',
    'state 1457:',
    '',
    '	select_statement__single_row : _SELECT set_quantifier_opt select_list _INTO select_target_list _ table_expression',
    '	select_target_list : select_target_list _ comma target_specification',
    '',
    '	comma	shift 1472',
    '	_FROM	shift 562',
    '	.	error',
    '',
    '	from_clause	goto 560',
    '	table_expression	goto 1471',
    '',
    'state 1458:',
    '',
    '	select_target_list : target_specification _	(801)',
    '',
    '	.	reduce 801',
    '',
    'state 1459:',
    '',
    '	update_statement__positioned : _UPDATE table_name _SET set_clause_list _WHERE _ _CURRENT _OF cursor_name',
    '	where_clause : _WHERE _ search_condition',
    '',
    '	identifier_body	shift 66',
    '	national_character_string_literal_start	shift 143',
    '	bit_string_literal_start	shift 144',
    '	string_literal_continuation	shift 145',
    '	hex_string_literal_start	shift 146',
    '	delimited_identifier	shift 67',
    '	digit	shift 147',
    '	left_paren	shift 636',
    '	plus_sign	shift 249',
    '	minus_sign	shift 250',
    '	period	shift 150',
    '	colon	shift 151',
    '	underscore	shift 69',
    '	_AVG	shift 251',
    '	_BIT_LENGTH	shift 252',
    '	_CASE	shift 253',
    '	_CAST	shift 254',
    '	_CHARACTER_LENGTH	shift 255',
    '	_CHAR_LENGTH	shift 256',
    '	_COALESCE	shift 257',
    '	_CONVERT	shift 258',
    '	_CURRENT	shift 1473',
    '	_CURRENT_DATE	shift 259',
    '	_CURRENT_TIME	shift 260',
    '	_CURRENT_TIMESTAMP	shift 261',
    '	_CURRENT_USER	shift 262',
    '	_DATE	shift 154',
    '	_DEFAULT	shift 263',
    '	_EXISTS	shift 637',
    '	_EXTRACT	shift 264',
    '	_INTERVAL	shift 156',
    '	_LOWER	shift 265',
    '	_MAX	shift 266',
    '	_MIN	shift 267',
    '	_NOT	shift 638',
    '	_NULL	shift 268',
    '	_NULLIF	shift 269',
    '	_OCTET_LENGTH	shift 270',
    '	_POSITION	shift 271',
    '	_SESSION_USER	shift 272',
    '	_SUBSTRING	shift 273',
    '	_SUM	shift 274',
    '	_SYSTEM_USER	shift 275',
    '	_TIME	shift 157',
    '	_TIMESTAMP	shift 158',
    '	_TRANSLATE	shift 276',
    '	_TRIM	shift 277',
    '	_UNIQUE	shift 639',
    '	_UPPER	shift 278',
    '	_USER	shift 279',
    '	_VALUE	shift 280',
    '	_COUNT	shift 281',
    '	.	error',
    '',
    '	row_value_constructor_1	goto 617',
    '	char_length_specifier	goto 200',
    '	bit_length_expression	goto 201',
    '	octet_length_expression	goto 202',
    '	char_length_expression	goto 203',
    '	trim_function	goto 204',
    '	character_translation	goto 205',
    '	form_of_use_conversion	goto 206',
    '	fold	goto 207',
    '	character_bit_substring_function	goto 208',
    '	length_expression	goto 209',
    '	extract_expression	goto 210',
    '	position_expression	goto 211',
    '	searched_case	goto 212',
    '	simple_case	goto 213',
    '	case_specification	goto 214',
    '	case_abbreviation	goto 215',
    '	set_function_type	goto 217',
    '	general_set_function	goto 218',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 220',
    '	general_value_specification	goto 221',
    '	unsigned_literal	goto 222',
    '	character_value_function	goto 223',
    '	multiplicative_expression	goto 224',
    '	unary_expression	goto 225',
    '	postfix_expression	goto 226',
    '	default_specification	goto 227',
    '	null_specification	goto 228',
    '	string_value_function	goto 229',
    '	numeric_value_function	goto 230',
    '	cast_specification	goto 231',
    '	case_expression	goto 232',
    '	scalar_subquery	goto 233',
    '	set_function_specification	goto 234',
    '	column_reference	goto 235',
    '	unsigned_value_specification	goto 236',
    '	primary_expression	goto 237',
    '	expression	goto 618',
    '	row_value_constructor	goto 619',
    '	overlaps_predicate	goto 620',
    '	match_predicate	goto 621',
    '	unique_predicate	goto 622',
    '	exists_predicate	goto 623',
    '	quantified_comparison_predicate	goto 624',
    '	null_predicate	goto 625',
    '	like_predicate	goto 626',
    '	in_predicate	goto 627',
    '	between_predicate	goto 628',
    '	comparison_predicate	goto 629',
    '	predicate	goto 630',
    '	boolean_primary	goto 631',
    '	boolean_test	goto 632',
    '	boolean_factor	goto 633',
    '	boolean_term	goto 634',
    '	search_condition	goto 766',
    '	current_timestamp_value_function	goto 240',
    '	current_time_value_function	goto 241',
    '	current_date_value_function	goto 242',
    '	timestamp_literal	goto 123',
    '	time_literal	goto 124',
    '	date_literal	goto 125',
    '	interval_literal	goto 126',
    '	datetime_literal	goto 127',
    '	general_literal	goto 243',
    '	datetime_value_function	goto 244',
    '	qualified_name	goto 245',
    '	actual_identifier	goto 61',
    '	identifier	goto 197',
    '	character_string_literal_main	goto 131',
    '	introducer	goto 246',
    '	character_string_literal	goto 133',
    '	hex_string_literal	goto 134',
    '	bit_string_literal	goto 135',
    '	national_character_string_literal	goto 136',
    '	mantissa	goto 138',
    '	unsigned_integer	goto 139',
    '	approximate_numeric_literal	goto 140',
    '	exact_numeric_literal	goto 141',
    '	unsigned_numeric_literal	goto 247',
    '	regular_identifier	goto 64',
    '',
    'state 1460:',
    '',
    '	referenced_table_and_columns : table_name reference_column_list_opt _	(243)',
    '',
    '	.	reduce 243',
    '',
    'state 1461:',
    '',
    '	reference_column_list_opt : left_paren _ reference_column_list right_paren',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	column_name_list	goto 1349',
    '	reference_column_list	goto 1474',
    '	column_name	goto 551',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1462:',
    '',
    '	references_specification : _REFERENCES referenced_table_and_columns match_type_opt _ referential_triggered_action_opt',
    '	referential_triggered_action_opt : _	(241)',
    '',
    '	_ON	shift 1479',
    '	$end	reduce 241',
    '	identifier_body	reduce 241',
    '	delimited_identifier	reduce 241',
    '	left_paren	reduce 241',
    '	right_paren	reduce 241',
    '	comma	reduce 241',
    '	semicolon	reduce 241',
    '	underscore	reduce 241',
    '	_ALTER	reduce 241',
    '	_COLLATE	reduce 241',
    '	_COMMIT	reduce 241',
    '	_CONNECT	reduce 241',
    '	_CREATE	reduce 241',
    '	_DECLARE	reduce 241',
    '	_DEFERRABLE	reduce 241',
    '	_DELETE	reduce 241',
    '	_DISCONNECT	reduce 241',
    '	_DROP	reduce 241',
    '	_GRANT	reduce 241',
    '	_INITIALLY	reduce 241',
    '	_INSERT	reduce 241',
    '	_REVOKE	reduce 241',
    '	_ROLLBACK	reduce 241',
    '	_SELECT	reduce 241',
    '	_SET	reduce 241',
    '	_TABLE	reduce 241',
    '	_UPDATE	reduce 241',
    '	_VALUES	reduce 241',
    '	.	error',
    '',
    '	delete_rule	goto 1475',
    '	update_rule	goto 1476',
    '	referential_triggered_action	goto 1477',
    '	referential_triggered_action_opt	goto 1478',
    '',
    'state 1463:',
    '',
    '	match_type_opt : _MATCH _ match_type',
    '',
    '	_FULL	shift 1481',
    '	_PARTIAL	shift 1482',
    '	.	error',
    '',
    '	match_type	goto 1480',
    '',
    'state 1464:',
    '',
    '	external_collation : _EXTERNAL left_paren quote external_collation_name quote right_paren _	(708)',
    '',
    '	.	reduce 708',
    '',
    'state 1465:',
    '',
    '	external_translation : _EXTERNAL left_paren quote external_translation_name _ quote right_paren',
    '',
    '	quote	shift 1483',
    '	.	error',
    '',
    'state 1466:',
    '',
    '	external_translation_name : translation_name _	(726)',
    '',
    '	.	reduce 726',
    '',
    'state 1467:',
    '',
    '	updatability_clause_opt : _FOR _READ _ _ONLY',
    '',
    '	_ONLY	shift 1484',
    '	.	error',
    '',
    'state 1468:',
    '',
    '	updatability_clause_opt : _FOR _UPDATE _ updatability_column_opt',
    '	updatability_column_opt : _	(600)',
    '',
    '	_OF	shift 1486',
    '	$end	reduce 600',
    '	.	error',
    '',
    '	updatability_column_opt	goto 1485',
    '',
    'state 1469:',
    '',
    '	delete_statement__positioned : _DELETE _FROM table_name _WHERE _CURRENT _OF _ cursor_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	cursor_name	goto 1487',
    '	actual_identifier	goto 61',
    '	identifier	goto 685',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1470:',
    '',
    '	fetch_target_list : fetch_target_list comma _ target_specification',
    '',
    '	colon	shift 151',
    '	.	error',
    '',
    '	target_specification	goto 1488',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 1456',
    '',
    'state 1471:',
    '',
    '	select_statement__single_row : _SELECT set_quantifier_opt select_list _INTO select_target_list table_expression _	(800)',
    '',
    '	.	reduce 800',
    '',
    'state 1472:',
    '',
    '	select_target_list : select_target_list comma _ target_specification',
    '',
    '	colon	shift 151',
    '	.	error',
    '',
    '	target_specification	goto 1489',
    '	parameter_name	goto 219',
    '	parameter_specification	goto 1456',
    '',
    'state 1473:',
    '',
    '	update_statement__positioned : _UPDATE table_name _SET set_clause_list _WHERE _CURRENT _ _OF cursor_name',
    '',
    '	_OF	shift 1490',
    '	.	error',
    '',
    'state 1474:',
    '',
    '	reference_column_list_opt : left_paren reference_column_list _ right_paren',
    '',
    '	right_paren	shift 1491',
    '	.	error',
    '',
    'state 1475:',
    '',
    '	referential_triggered_action : delete_rule _ update_rule_opt',
    '	update_rule_opt : _	(255)',
    '',
    '	_ON	shift 1494',
    '	$end	reduce 255',
    '	identifier_body	reduce 255',
    '	delimited_identifier	reduce 255',
    '	left_paren	reduce 255',
    '	right_paren	reduce 255',
    '	comma	reduce 255',
    '	semicolon	reduce 255',
    '	underscore	reduce 255',
    '	_ALTER	reduce 255',
    '	_COLLATE	reduce 255',
    '	_COMMIT	reduce 255',
    '	_CONNECT	reduce 255',
    '	_CREATE	reduce 255',
    '	_DECLARE	reduce 255',
    '	_DEFERRABLE	reduce 255',
    '	_DELETE	reduce 255',
    '	_DISCONNECT	reduce 255',
    '	_DROP	reduce 255',
    '	_GRANT	reduce 255',
    '	_INITIALLY	reduce 255',
    '	_INSERT	reduce 255',
    '	_REVOKE	reduce 255',
    '	_ROLLBACK	reduce 255',
    '	_SELECT	reduce 255',
    '	_SET	reduce 255',
    '	_TABLE	reduce 255',
    '	_UPDATE	reduce 255',
    '	_VALUES	reduce 255',
    '	.	error',
    '',
    '	update_rule_opt	goto 1492',
    '	update_rule	goto 1493',
    '',
    'state 1476:',
    '',
    '	referential_triggered_action : update_rule _ delete_rule_opt',
    '	delete_rule_opt : _	(257)',
    '',
    '	_ON	shift 1497',
    '	$end	reduce 257',
    '	identifier_body	reduce 257',
    '	delimited_identifier	reduce 257',
    '	left_paren	reduce 257',
    '	right_paren	reduce 257',
    '	comma	reduce 257',
    '	semicolon	reduce 257',
    '	underscore	reduce 257',
    '	_ALTER	reduce 257',
    '	_COLLATE	reduce 257',
    '	_COMMIT	reduce 257',
    '	_CONNECT	reduce 257',
    '	_CREATE	reduce 257',
    '	_DECLARE	reduce 257',
    '	_DEFERRABLE	reduce 257',
    '	_DELETE	reduce 257',
    '	_DISCONNECT	reduce 257',
    '	_DROP	reduce 257',
    '	_GRANT	reduce 257',
    '	_INITIALLY	reduce 257',
    '	_INSERT	reduce 257',
    '	_REVOKE	reduce 257',
    '	_ROLLBACK	reduce 257',
    '	_SELECT	reduce 257',
    '	_SET	reduce 257',
    '	_TABLE	reduce 257',
    '	_UPDATE	reduce 257',
    '	_VALUES	reduce 257',
    '	.	error',
    '',
    '	delete_rule	goto 1495',
    '	delete_rule_opt	goto 1496',
    '',
    'state 1477:',
    '',
    '	referential_triggered_action_opt : referential_triggered_action _	(242)',
    '',
    '	.	reduce 242',
    '',
    'state 1478:',
    '',
    '	references_specification : _REFERENCES referenced_table_and_columns match_type_opt referential_triggered_action_opt _	(238)',
    '',
    '	.	reduce 238',
    '',
    'state 1479:',
    '',
    '	update_rule : _ON _ _UPDATE referential_action',
    '	delete_rule : _ON _ _DELETE referential_action',
    '',
    '	_DELETE	shift 1498',
    '	_UPDATE	shift 1499',
    '	.	error',
    '',
    'state 1480:',
    '',
    '	match_type_opt : _MATCH match_type _	(240)',
    '',
    '	.	reduce 240',
    '',
    'state 1481:',
    '',
    '	match_type : _FULL _	(251)',
    '',
    '	.	reduce 251',
    '',
    'state 1482:',
    '',
    '	match_type : _PARTIAL _	(252)',
    '',
    '	.	reduce 252',
    '',
    'state 1483:',
    '',
    '	external_translation : _EXTERNAL left_paren quote external_translation_name quote _ right_paren',
    '',
    '	right_paren	shift 1500',
    '	.	error',
    '',
    'state 1484:',
    '',
    '	updatability_clause_opt : _FOR _READ _ONLY _	(598)',
    '',
    '	.	reduce 598',
    '',
    'state 1485:',
    '',
    '	updatability_clause_opt : _FOR _UPDATE updatability_column_opt _	(599)',
    '',
    '	.	reduce 599',
    '',
    'state 1486:',
    '',
    '	updatability_column_opt : _OF _ column_name_list',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	column_name_list	goto 1501',
    '	column_name	goto 551',
    '	actual_identifier	goto 61',
    '	identifier	goto 459',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1487:',
    '',
    '	delete_statement__positioned : _DELETE _FROM table_name _WHERE _CURRENT _OF cursor_name _	(808)',
    '',
    '	.	reduce 808',
    '',
    'state 1488:',
    '',
    '	fetch_target_list : fetch_target_list comma target_specification _	(797)',
    '',
    '	.	reduce 797',
    '',
    'state 1489:',
    '',
    '	select_target_list : select_target_list comma target_specification _	(802)',
    '',
    '	.	reduce 802',
    '',
    'state 1490:',
    '',
    '	update_statement__positioned : _UPDATE table_name _SET set_clause_list _WHERE _CURRENT _OF _ cursor_name',
    '',
    '	identifier_body	shift 66',
    '	delimited_identifier	shift 67',
    '	underscore	shift 69',
    '	.	error',
    '',
    '	cursor_name	goto 1502',
    '	actual_identifier	goto 61',
    '	identifier	goto 685',
    '	introducer	goto 63',
    '	regular_identifier	goto 64',
    '',
    'state 1491:',
    '',
    '	reference_column_list_opt : left_paren reference_column_list right_paren _	(245)',
    '',
    '	.	reduce 245',
    '',
    'state 1492:',
    '',
    '	referential_triggered_action : delete_rule update_rule_opt _	(254)',
    '',
    '	.	reduce 254',
    '',
    'state 1493:',
    '',
    '	update_rule_opt : update_rule _	(256)',
    '',
    '	.	reduce 256',
    '',
    'state 1494:',
    '',
    '	update_rule : _ON _ _UPDATE referential_action',
    '',
    '	_UPDATE	shift 1499',
    '	.	error',
    '',
    'state 1495:',
    '',
    '	delete_rule_opt : delete_rule _	(258)',
    '',
    '	.	reduce 258',
    '',
    'state 1496:',
    '',
    '	referential_triggered_action : update_rule delete_rule_opt _	(253)',
    '',
    '	.	reduce 253',
    '',
    'state 1497:',
    '',
    '	delete_rule : _ON _ _DELETE referential_action',
    '',
    '	_DELETE	shift 1498',
    '	.	error',
    '',
    'state 1498:',
    '',
    '	delete_rule : _ON _DELETE _ referential_action',
    '',
    '	_CASCADE	shift 1504',
    '	_NO	shift 1505',
    '	_SET	shift 1506',
    '	.	error',
    '',
    '	referential_action	goto 1503',
    '',
    'state 1499:',
    '',
    '	update_rule : _ON _UPDATE _ referential_action',
    '',
    '	_CASCADE	shift 1504',
    '	_NO	shift 1505',
    '	_SET	shift 1506',
    '	.	error',
    '',
    '	referential_action	goto 1507',
    '',
    'state 1500:',
    '',
    '	external_translation : _EXTERNAL left_paren quote external_translation_name quote right_paren _	(725)',
    '',
    '	.	reduce 725',
    '',
    'state 1501:',
    '',
    '	updatability_column_opt : _OF column_name_list _	(601)',
    '	column_name_list : column_name_list _ comma column_name',
    '',
    '	comma	shift 786',
    '	$end	reduce 601',
    '	.	error',
    '',
    'state 1502:',
    '',
    '	update_statement__positioned : _UPDATE table_name _SET set_clause_list _WHERE _CURRENT _OF cursor_name _	(815)',
    '',
    '	.	reduce 815',
    '',
    'state 1503:',
    '',
    '	delete_rule : _ON _DELETE referential_action _	(264)',
    '',
    '	.	reduce 264',
    '',
    'state 1504:',
    '',
    '	referential_action : _CASCADE _	(260)',
    '',
    '	.	reduce 260',
    '',
    'state 1505:',
    '',
    '	referential_action : _NO _ _ACTION',
    '',
    '	_ACTION	shift 1508',
    '	.	error',
    '',
    'state 1506:',
    '',
    '	referential_action : _SET _ _NULL',
    '	referential_action : _SET _ _DEFAULT',
    '',
    '	_DEFAULT	shift 1509',
    '	_NULL	shift 1510',
    '	.	error',
    '',
    'state 1507:',
    '',
    '	update_rule : _ON _UPDATE referential_action _	(259)',
    '',
    '	.	reduce 259',
    '',
    'state 1508:',
    '',
    '	referential_action : _NO _ACTION _	(263)',
    '',
    '	.	reduce 263',
    '',
    'state 1509:',
    '',
    '	referential_action : _SET _DEFAULT _	(262)',
    '',
    '	.	reduce 262',
    '',
    'state 1510:',
    '',
    '	referential_action : _SET _NULL _	(261)',
    '',
    '	.	reduce 261',
    '',
    '41 shift/reduce conflicts.');

function LookupOffsets(State: integer; var First: integer;var Last: integer): boolean;
begin
  case State of
    0: begin
      First := 1;
      Last := 94;
      result := true;
    end;
    1: begin
      First := 95;
      Last := 100;
      result := true;
    end;
    2: begin
      First := 101;
      Last := 190;
      result := true;
    end;
    3: begin
      First := 191;
      Last := 196;
      result := true;
    end;
    4: begin
      First := 197;
      Last := 202;
      result := true;
    end;
    5: begin
      First := 203;
      Last := 208;
      result := true;
    end;
    6: begin
      First := 209;
      Last := 214;
      result := true;
    end;
    7: begin
      First := 215;
      Last := 220;
      result := true;
    end;
    8: begin
      First := 221;
      Last := 226;
      result := true;
    end;
    9: begin
      First := 227;
      Last := 232;
      result := true;
    end;
    10: begin
      First := 233;
      Last := 238;
      result := true;
    end;
    11: begin
      First := 239;
      Last := 244;
      result := true;
    end;
    12: begin
      First := 245;
      Last := 250;
      result := true;
    end;
    13: begin
      First := 251;
      Last := 256;
      result := true;
    end;
    14: begin
      First := 257;
      Last := 262;
      result := true;
    end;
    15: begin
      First := 263;
      Last := 268;
      result := true;
    end;
    16: begin
      First := 269;
      Last := 274;
      result := true;
    end;
    17: begin
      First := 275;
      Last := 280;
      result := true;
    end;
    18: begin
      First := 281;
      Last := 286;
      result := true;
    end;
    19: begin
      First := 287;
      Last := 292;
      result := true;
    end;
    20: begin
      First := 293;
      Last := 298;
      result := true;
    end;
    21: begin
      First := 299;
      Last := 304;
      result := true;
    end;
    22: begin
      First := 305;
      Last := 310;
      result := true;
    end;
    23: begin
      First := 311;
      Last := 316;
      result := true;
    end;
    24: begin
      First := 317;
      Last := 322;
      result := true;
    end;
    25: begin
      First := 323;
      Last := 328;
      result := true;
    end;
    26: begin
      First := 329;
      Last := 334;
      result := true;
    end;
    27: begin
      First := 335;
      Last := 340;
      result := true;
    end;
    28: begin
      First := 341;
      Last := 346;
      result := true;
    end;
    29: begin
      First := 347;
      Last := 352;
      result := true;
    end;
    30: begin
      First := 353;
      Last := 358;
      result := true;
    end;
    31: begin
      First := 359;
      Last := 364;
      result := true;
    end;
    32: begin
      First := 365;
      Last := 370;
      result := true;
    end;
    33: begin
      First := 371;
      Last := 376;
      result := true;
    end;
    34: begin
      First := 377;
      Last := 382;
      result := true;
    end;
    35: begin
      First := 383;
      Last := 388;
      result := true;
    end;
    36: begin
      First := 389;
      Last := 394;
      result := true;
    end;
    37: begin
      First := 395;
      Last := 400;
      result := true;
    end;
    38: begin
      First := 401;
      Last := 406;
      result := true;
    end;
    39: begin
      First := 407;
      Last := 412;
      result := true;
    end;
    40: begin
      First := 413;
      Last := 418;
      result := true;
    end;
    41: begin
      First := 419;
      Last := 424;
      result := true;
    end;
    42: begin
      First := 425;
      Last := 430;
      result := true;
    end;
    43: begin
      First := 431;
      Last := 436;
      result := true;
    end;
    44: begin
      First := 437;
      Last := 442;
      result := true;
    end;
    45: begin
      First := 443;
      Last := 448;
      result := true;
    end;
    46: begin
      First := 449;
      Last := 454;
      result := true;
    end;
    47: begin
      First := 455;
      Last := 460;
      result := true;
    end;
    48: begin
      First := 461;
      Last := 466;
      result := true;
    end;
    49: begin
      First := 467;
      Last := 472;
      result := true;
    end;
    50: begin
      First := 473;
      Last := 478;
      result := true;
    end;
    51: begin
      First := 479;
      Last := 484;
      result := true;
    end;
    52: begin
      First := 485;
      Last := 490;
      result := true;
    end;
    53: begin
      First := 491;
      Last := 496;
      result := true;
    end;
    54: begin
      First := 497;
      Last := 502;
      result := true;
    end;
    55: begin
      First := 503;
      Last := 509;
      result := true;
    end;
    56: begin
      First := 510;
      Last := 546;
      result := true;
    end;
    57: begin
      First := 547;
      Last := 582;
      result := true;
    end;
    58: begin
      First := 583;
      Last := 588;
      result := true;
    end;
    59: begin
      First := 589;
      Last := 597;
      result := true;
    end;
    60: begin
      First := 598;
      Last := 603;
      result := true;
    end;
    61: begin
      First := 604;
      Last := 609;
      result := true;
    end;
    62: begin
      First := 610;
      Last := 615;
      result := true;
    end;
    63: begin
      First := 616;
      Last := 632;
      result := true;
    end;
    64: begin
      First := 633;
      Last := 638;
      result := true;
    end;
    65: begin
      First := 639;
      Last := 645;
      result := true;
    end;
    66: begin
      First := 646;
      Last := 651;
      result := true;
    end;
    67: begin
      First := 652;
      Last := 657;
      result := true;
    end;
    68: begin
      First := 658;
      Last := 677;
      result := true;
    end;
    69: begin
      First := 678;
      Last := 683;
      result := true;
    end;
    70: begin
      First := 684;
      Last := 692;
      result := true;
    end;
    71: begin
      First := 693;
      Last := 723;
      result := true;
    end;
    72: begin
      First := 724;
      Last := 730;
      result := true;
    end;
    73: begin
      First := 731;
      Last := 756;
      result := true;
    end;
    74: begin
      First := 757;
      Last := 763;
      result := true;
    end;
    75: begin
      First := 764;
      Last := 770;
      result := true;
    end;
    76: begin
      First := 771;
      Last := 819;
      result := true;
    end;
    77: begin
      First := 820;
      Last := 840;
      result := true;
    end;
    78: begin
      First := 841;
      Last := 857;
      result := true;
    end;
    79: begin
      First := 858;
      Last := 864;
      result := true;
    end;
    80: begin
      First := 865;
      Last := 871;
      result := true;
    end;
    81: begin
      First := 872;
      Last := 888;
      result := true;
    end;
    82: begin
      First := 889;
      Last := 919;
      result := true;
    end;
    83: begin
      First := 920;
      Last := 980;
      result := true;
    end;
    84: begin
      First := 981;
      Last := 1002;
      result := true;
    end;
    85: begin
      First := 1003;
      Last := 1020;
      result := true;
    end;
    86: begin
      First := 1021;
      Last := 1038;
      result := true;
    end;
    87: begin
      First := 1039;
      Last := 1158;
      result := true;
    end;
    88: begin
      First := 1159;
      Last := 1164;
      result := true;
    end;
    89: begin
      First := 1165;
      Last := 1179;
      result := true;
    end;
    90: begin
      First := 1180;
      Last := 1185;
      result := true;
    end;
    91: begin
      First := 1186;
      Last := 1200;
      result := true;
    end;
    92: begin
      First := 1201;
      Last := 1207;
      result := true;
    end;
    93: begin
      First := 1208;
      Last := 1222;
      result := true;
    end;
    94: begin
      First := 1223;
      Last := 1232;
      result := true;
    end;
    95: begin
      First := 1233;
      Last := 1247;
      result := true;
    end;
    96: begin
      First := 1248;
      Last := 1253;
      result := true;
    end;
    97: begin
      First := 1254;
      Last := 1261;
      result := true;
    end;
    98: begin
      First := 1262;
      Last := 1267;
      result := true;
    end;
    99: begin
      First := 1268;
      Last := 1278;
      result := true;
    end;
    100: begin
      First := 1279;
      Last := 1327;
      result := true;
    end;
    101: begin
      First := 1328;
      Last := 1338;
      result := true;
    end;
    102: begin
      First := 1339;
      Last := 1354;
      result := true;
    end;
    103: begin
      First := 1355;
      Last := 1372;
      result := true;
    end;
    104: begin
      First := 1373;
      Last := 1378;
      result := true;
    end;
    105: begin
      First := 1379;
      Last := 1424;
      result := true;
    end;
    106: begin
      First := 1425;
      Last := 1431;
      result := true;
    end;
    107: begin
      First := 1432;
      Last := 1447;
      result := true;
    end;
    108: begin
      First := 1448;
      Last := 1454;
      result := true;
    end;
    109: begin
      First := 1455;
      Last := 1470;
      result := true;
    end;
    110: begin
      First := 1471;
      Last := 1486;
      result := true;
    end;
    111: begin
      First := 1487;
      Last := 1493;
      result := true;
    end;
    112: begin
      First := 1494;
      Last := 1500;
      result := true;
    end;
    113: begin
      First := 1501;
      Last := 1517;
      result := true;
    end;
    114: begin
      First := 1518;
      Last := 1533;
      result := true;
    end;
    115: begin
      First := 1534;
      Last := 1551;
      result := true;
    end;
    116: begin
      First := 1552;
      Last := 1558;
      result := true;
    end;
    117: begin
      First := 1559;
      Last := 1576;
      result := true;
    end;
    118: begin
      First := 1577;
      Last := 1582;
      result := true;
    end;
    119: begin
      First := 1583;
      Last := 1588;
      result := true;
    end;
    120: begin
      First := 1589;
      Last := 1594;
      result := true;
    end;
    121: begin
      First := 1595;
      Last := 1600;
      result := true;
    end;
    122: begin
      First := 1601;
      Last := 1606;
      result := true;
    end;
    123: begin
      First := 1607;
      Last := 1612;
      result := true;
    end;
    124: begin
      First := 1613;
      Last := 1618;
      result := true;
    end;
    125: begin
      First := 1619;
      Last := 1624;
      result := true;
    end;
    126: begin
      First := 1625;
      Last := 1630;
      result := true;
    end;
    127: begin
      First := 1631;
      Last := 1636;
      result := true;
    end;
    128: begin
      First := 1637;
      Last := 1642;
      result := true;
    end;
    129: begin
      First := 1643;
      Last := 1648;
      result := true;
    end;
    130: begin
      First := 1649;
      Last := 1654;
      result := true;
    end;
    131: begin
      First := 1655;
      Last := 1746;
      result := true;
    end;
    132: begin
      First := 1747;
      Last := 1763;
      result := true;
    end;
    133: begin
      First := 1764;
      Last := 1769;
      result := true;
    end;
    134: begin
      First := 1770;
      Last := 1775;
      result := true;
    end;
    135: begin
      First := 1776;
      Last := 1781;
      result := true;
    end;
    136: begin
      First := 1782;
      Last := 1787;
      result := true;
    end;
    137: begin
      First := 1788;
      Last := 1801;
      result := true;
    end;
    138: begin
      First := 1802;
      Last := 1808;
      result := true;
    end;
    139: begin
      First := 1809;
      Last := 1905;
      result := true;
    end;
    140: begin
      First := 1906;
      Last := 1911;
      result := true;
    end;
    141: begin
      First := 1912;
      Last := 2003;
      result := true;
    end;
    142: begin
      First := 2004;
      Last := 2009;
      result := true;
    end;
    143: begin
      First := 2010;
      Last := 2018;
      result := true;
    end;
    144: begin
      First := 2019;
      Last := 2027;
      result := true;
    end;
    145: begin
      First := 2028;
      Last := 2033;
      result := true;
    end;
    146: begin
      First := 2034;
      Last := 2042;
      result := true;
    end;
    147: begin
      First := 2043;
      Last := 2048;
      result := true;
    end;
    148: begin
      First := 2049;
      Last := 2054;
      result := true;
    end;
    149: begin
      First := 2055;
      Last := 2060;
      result := true;
    end;
    150: begin
      First := 2061;
      Last := 2069;
      result := true;
    end;
    151: begin
      First := 2070;
      Last := 2083;
      result := true;
    end;
    152: begin
      First := 2084;
      Last := 2089;
      result := true;
    end;
    153: begin
      First := 2090;
      Last := 2095;
      result := true;
    end;
    154: begin
      First := 2096;
      Last := 2104;
      result := true;
    end;
    155: begin
      First := 2105;
      Last := 2110;
      result := true;
    end;
    156: begin
      First := 2111;
      Last := 2123;
      result := true;
    end;
    157: begin
      First := 2124;
      Last := 2132;
      result := true;
    end;
    158: begin
      First := 2133;
      Last := 2141;
      result := true;
    end;
    159: begin
      First := 2142;
      Last := 2157;
      result := true;
    end;
    160: begin
      First := 2158;
      Last := 2164;
      result := true;
    end;
    161: begin
      First := 2165;
      Last := 2180;
      result := true;
    end;
    162: begin
      First := 2181;
      Last := 2196;
      result := true;
    end;
    163: begin
      First := 2197;
      Last := 2211;
      result := true;
    end;
    164: begin
      First := 2212;
      Last := 2229;
      result := true;
    end;
    165: begin
      First := 2230;
      Last := 2245;
      result := true;
    end;
    166: begin
      First := 2246;
      Last := 2263;
      result := true;
    end;
    167: begin
      First := 2264;
      Last := 2269;
      result := true;
    end;
    168: begin
      First := 2270;
      Last := 2278;
      result := true;
    end;
    169: begin
      First := 2279;
      Last := 2285;
      result := true;
    end;
    170: begin
      First := 2286;
      Last := 2292;
      result := true;
    end;
    171: begin
      First := 2293;
      Last := 2298;
      result := true;
    end;
    172: begin
      First := 2299;
      Last := 2310;
      result := true;
    end;
    173: begin
      First := 2311;
      Last := 2322;
      result := true;
    end;
    174: begin
      First := 2323;
      Last := 2328;
      result := true;
    end;
    175: begin
      First := 2329;
      Last := 2340;
      result := true;
    end;
    176: begin
      First := 2341;
      Last := 2346;
      result := true;
    end;
    177: begin
      First := 2347;
      Last := 2364;
      result := true;
    end;
    178: begin
      First := 2365;
      Last := 2379;
      result := true;
    end;
    179: begin
      First := 2380;
      Last := 2396;
      result := true;
    end;
    180: begin
      First := 2397;
      Last := 2403;
      result := true;
    end;
    181: begin
      First := 2404;
      Last := 2409;
      result := true;
    end;
    182: begin
      First := 2410;
      Last := 2533;
      result := true;
    end;
    183: begin
      First := 2534;
      Last := 2539;
      result := true;
    end;
    184: begin
      First := 2540;
      Last := 2545;
      result := true;
    end;
    185: begin
      First := 2546;
      Last := 2551;
      result := true;
    end;
    186: begin
      First := 2552;
      Last := 2601;
      result := true;
    end;
    187: begin
      First := 2602;
      Last := 2647;
      result := true;
    end;
    188: begin
      First := 2648;
      Last := 2667;
      result := true;
    end;
    189: begin
      First := 2668;
      Last := 2717;
      result := true;
    end;
    190: begin
      First := 2718;
      Last := 2767;
      result := true;
    end;
    191: begin
      First := 2768;
      Last := 2774;
      result := true;
    end;
    192: begin
      First := 2775;
      Last := 2781;
      result := true;
    end;
    193: begin
      First := 2782;
      Last := 2796;
      result := true;
    end;
    194: begin
      First := 2797;
      Last := 2802;
      result := true;
    end;
    195: begin
      First := 2803;
      Last := 2808;
      result := true;
    end;
    196: begin
      First := 2809;
      Last := 2814;
      result := true;
    end;
    197: begin
      First := 2815;
      Last := 2941;
      result := true;
    end;
    198: begin
      First := 2942;
      Last := 2948;
      result := true;
    end;
    199: begin
      First := 2949;
      Last := 2955;
      result := true;
    end;
    200: begin
      First := 2956;
      Last := 2962;
      result := true;
    end;
    201: begin
      First := 2963;
      Last := 2968;
      result := true;
    end;
    202: begin
      First := 2969;
      Last := 2974;
      result := true;
    end;
    203: begin
      First := 2975;
      Last := 2980;
      result := true;
    end;
    204: begin
      First := 2981;
      Last := 2986;
      result := true;
    end;
    205: begin
      First := 2987;
      Last := 2992;
      result := true;
    end;
    206: begin
      First := 2993;
      Last := 2998;
      result := true;
    end;
    207: begin
      First := 2999;
      Last := 3004;
      result := true;
    end;
    208: begin
      First := 3005;
      Last := 3010;
      result := true;
    end;
    209: begin
      First := 3011;
      Last := 3016;
      result := true;
    end;
    210: begin
      First := 3017;
      Last := 3022;
      result := true;
    end;
    211: begin
      First := 3023;
      Last := 3028;
      result := true;
    end;
    212: begin
      First := 3029;
      Last := 3034;
      result := true;
    end;
    213: begin
      First := 3035;
      Last := 3040;
      result := true;
    end;
    214: begin
      First := 3041;
      Last := 3046;
      result := true;
    end;
    215: begin
      First := 3047;
      Last := 3052;
      result := true;
    end;
    216: begin
      First := 3053;
      Last := 3090;
      result := true;
    end;
    217: begin
      First := 3091;
      Last := 3097;
      result := true;
    end;
    218: begin
      First := 3098;
      Last := 3103;
      result := true;
    end;
    219: begin
      First := 3104;
      Last := 3193;
      result := true;
    end;
    220: begin
      First := 3194;
      Last := 3199;
      result := true;
    end;
    221: begin
      First := 3200;
      Last := 3205;
      result := true;
    end;
    222: begin
      First := 3206;
      Last := 3211;
      result := true;
    end;
    223: begin
      First := 3212;
      Last := 3217;
      result := true;
    end;
    224: begin
      First := 3218;
      Last := 3300;
      result := true;
    end;
    225: begin
      First := 3301;
      Last := 3306;
      result := true;
    end;
    226: begin
      First := 3307;
      Last := 3312;
      result := true;
    end;
    227: begin
      First := 3313;
      Last := 3318;
      result := true;
    end;
    228: begin
      First := 3319;
      Last := 3324;
      result := true;
    end;
    229: begin
      First := 3325;
      Last := 3330;
      result := true;
    end;
    230: begin
      First := 3331;
      Last := 3336;
      result := true;
    end;
    231: begin
      First := 3337;
      Last := 3342;
      result := true;
    end;
    232: begin
      First := 3343;
      Last := 3348;
      result := true;
    end;
    233: begin
      First := 3349;
      Last := 3354;
      result := true;
    end;
    234: begin
      First := 3355;
      Last := 3360;
      result := true;
    end;
    235: begin
      First := 3361;
      Last := 3366;
      result := true;
    end;
    236: begin
      First := 3367;
      Last := 3372;
      result := true;
    end;
    237: begin
      First := 3373;
      Last := 3464;
      result := true;
    end;
    238: begin
      First := 3465;
      Last := 3521;
      result := true;
    end;
    239: begin
      First := 3522;
      Last := 3527;
      result := true;
    end;
    240: begin
      First := 3528;
      Last := 3533;
      result := true;
    end;
    241: begin
      First := 3534;
      Last := 3539;
      result := true;
    end;
    242: begin
      First := 3540;
      Last := 3545;
      result := true;
    end;
    243: begin
      First := 3546;
      Last := 3551;
      result := true;
    end;
    244: begin
      First := 3552;
      Last := 3557;
      result := true;
    end;
    245: begin
      First := 3558;
      Last := 3563;
      result := true;
    end;
    246: begin
      First := 3564;
      Last := 3581;
      result := true;
    end;
    247: begin
      First := 3582;
      Last := 3587;
      result := true;
    end;
    248: begin
      First := 3588;
      Last := 3721;
      result := true;
    end;
    249: begin
      First := 3722;
      Last := 3834;
      result := true;
    end;
    250: begin
      First := 3835;
      Last := 3947;
      result := true;
    end;
    251: begin
      First := 3948;
      Last := 3953;
      result := true;
    end;
    252: begin
      First := 3954;
      Last := 3960;
      result := true;
    end;
    253: begin
      First := 3961;
      Last := 4082;
      result := true;
    end;
    254: begin
      First := 4083;
      Last := 4089;
      result := true;
    end;
    255: begin
      First := 4090;
      Last := 4095;
      result := true;
    end;
    256: begin
      First := 4096;
      Last := 4101;
      result := true;
    end;
    257: begin
      First := 4102;
      Last := 4108;
      result := true;
    end;
    258: begin
      First := 4109;
      Last := 4115;
      result := true;
    end;
    259: begin
      First := 4116;
      Last := 4121;
      result := true;
    end;
    260: begin
      First := 4122;
      Last := 4215;
      result := true;
    end;
    261: begin
      First := 4216;
      Last := 4309;
      result := true;
    end;
    262: begin
      First := 4310;
      Last := 4315;
      result := true;
    end;
    263: begin
      First := 4316;
      Last := 4321;
      result := true;
    end;
    264: begin
      First := 4322;
      Last := 4328;
      result := true;
    end;
    265: begin
      First := 4329;
      Last := 4335;
      result := true;
    end;
    266: begin
      First := 4336;
      Last := 4341;
      result := true;
    end;
    267: begin
      First := 4342;
      Last := 4347;
      result := true;
    end;
    268: begin
      First := 4348;
      Last := 4353;
      result := true;
    end;
    269: begin
      First := 4354;
      Last := 4360;
      result := true;
    end;
    270: begin
      First := 4361;
      Last := 4367;
      result := true;
    end;
    271: begin
      First := 4368;
      Last := 4374;
      result := true;
    end;
    272: begin
      First := 4375;
      Last := 4380;
      result := true;
    end;
    273: begin
      First := 4381;
      Last := 4387;
      result := true;
    end;
    274: begin
      First := 4388;
      Last := 4393;
      result := true;
    end;
    275: begin
      First := 4394;
      Last := 4399;
      result := true;
    end;
    276: begin
      First := 4400;
      Last := 4406;
      result := true;
    end;
    277: begin
      First := 4407;
      Last := 4413;
      result := true;
    end;
    278: begin
      First := 4414;
      Last := 4420;
      result := true;
    end;
    279: begin
      First := 4421;
      Last := 4426;
      result := true;
    end;
    280: begin
      First := 4427;
      Last := 4432;
      result := true;
    end;
    281: begin
      First := 4433;
      Last := 4438;
      result := true;
    end;
    282: begin
      First := 4439;
      Last := 4453;
      result := true;
    end;
    283: begin
      First := 4454;
      Last := 4459;
      result := true;
    end;
    284: begin
      First := 4460;
      Last := 4474;
      result := true;
    end;
    285: begin
      First := 4475;
      Last := 4494;
      result := true;
    end;
    286: begin
      First := 4495;
      Last := 4509;
      result := true;
    end;
    287: begin
      First := 4510;
      Last := 4524;
      result := true;
    end;
    288: begin
      First := 4525;
      Last := 4540;
      result := true;
    end;
    289: begin
      First := 4541;
      Last := 4556;
      result := true;
    end;
    290: begin
      First := 4557;
      Last := 4562;
      result := true;
    end;
    291: begin
      First := 4563;
      Last := 4568;
      result := true;
    end;
    292: begin
      First := 4569;
      Last := 4574;
      result := true;
    end;
    293: begin
      First := 4575;
      Last := 4580;
      result := true;
    end;
    294: begin
      First := 4581;
      Last := 4586;
      result := true;
    end;
    295: begin
      First := 4587;
      Last := 4592;
      result := true;
    end;
    296: begin
      First := 4593;
      Last := 4598;
      result := true;
    end;
    297: begin
      First := 4599;
      Last := 4604;
      result := true;
    end;
    298: begin
      First := 4605;
      Last := 4620;
      result := true;
    end;
    299: begin
      First := 4621;
      Last := 4626;
      result := true;
    end;
    300: begin
      First := 4627;
      Last := 4632;
      result := true;
    end;
    301: begin
      First := 4633;
      Last := 4638;
      result := true;
    end;
    302: begin
      First := 4639;
      Last := 4653;
      result := true;
    end;
    303: begin
      First := 4654;
      Last := 4669;
      result := true;
    end;
    304: begin
      First := 4670;
      Last := 4703;
      result := true;
    end;
    305: begin
      First := 4704;
      Last := 4709;
      result := true;
    end;
    306: begin
      First := 4710;
      Last := 4715;
      result := true;
    end;
    307: begin
      First := 4716;
      Last := 4721;
      result := true;
    end;
    308: begin
      First := 4722;
      Last := 4739;
      result := true;
    end;
    309: begin
      First := 4740;
      Last := 4748;
      result := true;
    end;
    310: begin
      First := 4749;
      Last := 4754;
      result := true;
    end;
    311: begin
      First := 4755;
      Last := 4770;
      result := true;
    end;
    312: begin
      First := 4771;
      Last := 4777;
      result := true;
    end;
    313: begin
      First := 4778;
      Last := 4783;
      result := true;
    end;
    314: begin
      First := 4784;
      Last := 4812;
      result := true;
    end;
    315: begin
      First := 4813;
      Last := 4818;
      result := true;
    end;
    316: begin
      First := 4819;
      Last := 4824;
      result := true;
    end;
    317: begin
      First := 4825;
      Last := 4837;
      result := true;
    end;
    318: begin
      First := 4838;
      Last := 4848;
      result := true;
    end;
    319: begin
      First := 4849;
      Last := 4864;
      result := true;
    end;
    320: begin
      First := 4865;
      Last := 4880;
      result := true;
    end;
    321: begin
      First := 4881;
      Last := 4887;
      result := true;
    end;
    322: begin
      First := 4888;
      Last := 4893;
      result := true;
    end;
    323: begin
      First := 4894;
      Last := 4904;
      result := true;
    end;
    324: begin
      First := 4905;
      Last := 4911;
      result := true;
    end;
    325: begin
      First := 4912;
      Last := 4944;
      result := true;
    end;
    326: begin
      First := 4945;
      Last := 4950;
      result := true;
    end;
    327: begin
      First := 4951;
      Last := 4959;
      result := true;
    end;
    328: begin
      First := 4960;
      Last := 4965;
      result := true;
    end;
    329: begin
      First := 4966;
      Last := 4979;
      result := true;
    end;
    330: begin
      First := 4980;
      Last := 4985;
      result := true;
    end;
    331: begin
      First := 4986;
      Last := 4991;
      result := true;
    end;
    332: begin
      First := 4992;
      Last := 5086;
      result := true;
    end;
    333: begin
      First := 5087;
      Last := 5178;
      result := true;
    end;
    334: begin
      First := 5179;
      Last := 5270;
      result := true;
    end;
    335: begin
      First := 5271;
      Last := 5362;
      result := true;
    end;
    336: begin
      First := 5363;
      Last := 5455;
      result := true;
    end;
    337: begin
      First := 5456;
      Last := 5461;
      result := true;
    end;
    338: begin
      First := 5462;
      Last := 5467;
      result := true;
    end;
    339: begin
      First := 5468;
      Last := 5477;
      result := true;
    end;
    340: begin
      First := 5478;
      Last := 5493;
      result := true;
    end;
    341: begin
      First := 5494;
      Last := 5502;
      result := true;
    end;
    342: begin
      First := 5503;
      Last := 5512;
      result := true;
    end;
    343: begin
      First := 5513;
      Last := 5518;
      result := true;
    end;
    344: begin
      First := 5519;
      Last := 5528;
      result := true;
    end;
    345: begin
      First := 5529;
      Last := 5534;
      result := true;
    end;
    346: begin
      First := 5535;
      Last := 5545;
      result := true;
    end;
    347: begin
      First := 5546;
      Last := 5551;
      result := true;
    end;
    348: begin
      First := 5552;
      Last := 5567;
      result := true;
    end;
    349: begin
      First := 5568;
      Last := 5573;
      result := true;
    end;
    350: begin
      First := 5574;
      Last := 5583;
      result := true;
    end;
    351: begin
      First := 5584;
      Last := 5593;
      result := true;
    end;
    352: begin
      First := 5594;
      Last := 5603;
      result := true;
    end;
    353: begin
      First := 5604;
      Last := 5609;
      result := true;
    end;
    354: begin
      First := 5610;
      Last := 5619;
      result := true;
    end;
    355: begin
      First := 5620;
      Last := 5633;
      result := true;
    end;
    356: begin
      First := 5634;
      Last := 5652;
      result := true;
    end;
    357: begin
      First := 5653;
      Last := 5658;
      result := true;
    end;
    358: begin
      First := 5659;
      Last := 5664;
      result := true;
    end;
    359: begin
      First := 5665;
      Last := 5681;
      result := true;
    end;
    360: begin
      First := 5682;
      Last := 5687;
      result := true;
    end;
    361: begin
      First := 5688;
      Last := 5693;
      result := true;
    end;
    362: begin
      First := 5694;
      Last := 5715;
      result := true;
    end;
    363: begin
      First := 5716;
      Last := 5722;
      result := true;
    end;
    364: begin
      First := 5723;
      Last := 5728;
      result := true;
    end;
    365: begin
      First := 5729;
      Last := 5735;
      result := true;
    end;
    366: begin
      First := 5736;
      Last := 5742;
      result := true;
    end;
    367: begin
      First := 5743;
      Last := 5748;
      result := true;
    end;
    368: begin
      First := 5749;
      Last := 5754;
      result := true;
    end;
    369: begin
      First := 5755;
      Last := 5764;
      result := true;
    end;
    370: begin
      First := 5765;
      Last := 5774;
      result := true;
    end;
    371: begin
      First := 5775;
      Last := 5801;
      result := true;
    end;
    372: begin
      First := 5802;
      Last := 5807;
      result := true;
    end;
    373: begin
      First := 5808;
      Last := 5839;
      result := true;
    end;
    374: begin
      First := 5840;
      Last := 5971;
      result := true;
    end;
    375: begin
      First := 5972;
      Last := 5977;
      result := true;
    end;
    376: begin
      First := 5978;
      Last := 5983;
      result := true;
    end;
    377: begin
      First := 5984;
      Last := 5989;
      result := true;
    end;
    378: begin
      First := 5990;
      Last := 5995;
      result := true;
    end;
    379: begin
      First := 5996;
      Last := 6001;
      result := true;
    end;
    380: begin
      First := 6002;
      Last := 6011;
      result := true;
    end;
    381: begin
      First := 6012;
      Last := 6020;
      result := true;
    end;
    382: begin
      First := 6021;
      Last := 6026;
      result := true;
    end;
    383: begin
      First := 6027;
      Last := 6032;
      result := true;
    end;
    384: begin
      First := 6033;
      Last := 6038;
      result := true;
    end;
    385: begin
      First := 6039;
      Last := 6044;
      result := true;
    end;
    386: begin
      First := 6045;
      Last := 6094;
      result := true;
    end;
    387: begin
      First := 6095;
      Last := 6214;
      result := true;
    end;
    388: begin
      First := 6215;
      Last := 6220;
      result := true;
    end;
    389: begin
      First := 6221;
      Last := 6226;
      result := true;
    end;
    390: begin
      First := 6227;
      Last := 6232;
      result := true;
    end;
    391: begin
      First := 6233;
      Last := 6238;
      result := true;
    end;
    392: begin
      First := 6239;
      Last := 6269;
      result := true;
    end;
    393: begin
      First := 6270;
      Last := 6276;
      result := true;
    end;
    394: begin
      First := 6277;
      Last := 6283;
      result := true;
    end;
    395: begin
      First := 6284;
      Last := 6292;
      result := true;
    end;
    396: begin
      First := 6293;
      Last := 6307;
      result := true;
    end;
    397: begin
      First := 6308;
      Last := 6322;
      result := true;
    end;
    398: begin
      First := 6323;
      Last := 6340;
      result := true;
    end;
    399: begin
      First := 6341;
      Last := 6458;
      result := true;
    end;
    400: begin
      First := 6459;
      Last := 6577;
      result := true;
    end;
    401: begin
      First := 6578;
      Last := 6591;
      result := true;
    end;
    402: begin
      First := 6592;
      Last := 6597;
      result := true;
    end;
    403: begin
      First := 6598;
      Last := 6603;
      result := true;
    end;
    404: begin
      First := 6604;
      Last := 6612;
      result := true;
    end;
    405: begin
      First := 6613;
      Last := 6728;
      result := true;
    end;
    406: begin
      First := 6729;
      Last := 6844;
      result := true;
    end;
    407: begin
      First := 6845;
      Last := 6850;
      result := true;
    end;
    408: begin
      First := 6851;
      Last := 6856;
      result := true;
    end;
    409: begin
      First := 6857;
      Last := 6953;
      result := true;
    end;
    410: begin
      First := 6954;
      Last := 7046;
      result := true;
    end;
    411: begin
      First := 7047;
      Last := 7052;
      result := true;
    end;
    412: begin
      First := 7053;
      Last := 7058;
      result := true;
    end;
    413: begin
      First := 7059;
      Last := 7068;
      result := true;
    end;
    414: begin
      First := 7069;
      Last := 7084;
      result := true;
    end;
    415: begin
      First := 7085;
      Last := 7090;
      result := true;
    end;
    416: begin
      First := 7091;
      Last := 7096;
      result := true;
    end;
    417: begin
      First := 7097;
      Last := 7102;
      result := true;
    end;
    418: begin
      First := 7103;
      Last := 7108;
      result := true;
    end;
    419: begin
      First := 7109;
      Last := 7206;
      result := true;
    end;
    420: begin
      First := 7207;
      Last := 7212;
      result := true;
    end;
    421: begin
      First := 7213;
      Last := 7329;
      result := true;
    end;
    422: begin
      First := 7330;
      Last := 7446;
      result := true;
    end;
    423: begin
      First := 7447;
      Last := 7563;
      result := true;
    end;
    424: begin
      First := 7564;
      Last := 7577;
      result := true;
    end;
    425: begin
      First := 7578;
      Last := 7588;
      result := true;
    end;
    426: begin
      First := 7589;
      Last := 7595;
      result := true;
    end;
    427: begin
      First := 7596;
      Last := 7604;
      result := true;
    end;
    428: begin
      First := 7605;
      Last := 7623;
      result := true;
    end;
    429: begin
      First := 7624;
      Last := 7756;
      result := true;
    end;
    430: begin
      First := 7757;
      Last := 7762;
      result := true;
    end;
    431: begin
      First := 7763;
      Last := 7768;
      result := true;
    end;
    432: begin
      First := 7769;
      Last := 7886;
      result := true;
    end;
    433: begin
      First := 7887;
      Last := 7898;
      result := true;
    end;
    434: begin
      First := 7899;
      Last := 7907;
      result := true;
    end;
    435: begin
      First := 7908;
      Last := 7920;
      result := true;
    end;
    436: begin
      First := 7921;
      Last := 8059;
      result := true;
    end;
    437: begin
      First := 8060;
      Last := 8178;
      result := true;
    end;
    438: begin
      First := 8179;
      Last := 8297;
      result := true;
    end;
    439: begin
      First := 8298;
      Last := 8415;
      result := true;
    end;
    440: begin
      First := 8416;
      Last := 8426;
      result := true;
    end;
    441: begin
      First := 8427;
      Last := 8437;
      result := true;
    end;
    442: begin
      First := 8438;
      Last := 8456;
      result := true;
    end;
    443: begin
      First := 8457;
      Last := 8574;
      result := true;
    end;
    444: begin
      First := 8575;
      Last := 8692;
      result := true;
    end;
    445: begin
      First := 8693;
      Last := 8810;
      result := true;
    end;
    446: begin
      First := 8811;
      Last := 8928;
      result := true;
    end;
    447: begin
      First := 8929;
      Last := 9046;
      result := true;
    end;
    448: begin
      First := 9047;
      Last := 9164;
      result := true;
    end;
    449: begin
      First := 9165;
      Last := 9289;
      result := true;
    end;
    450: begin
      First := 9290;
      Last := 9407;
      result := true;
    end;
    451: begin
      First := 9408;
      Last := 9413;
      result := true;
    end;
    452: begin
      First := 9414;
      Last := 9431;
      result := true;
    end;
    453: begin
      First := 9432;
      Last := 9445;
      result := true;
    end;
    454: begin
      First := 9446;
      Last := 9464;
      result := true;
    end;
    455: begin
      First := 9465;
      Last := 9501;
      result := true;
    end;
    456: begin
      First := 9502;
      Last := 9507;
      result := true;
    end;
    457: begin
      First := 9508;
      Last := 9538;
      result := true;
    end;
    458: begin
      First := 9539;
      Last := 9544;
      result := true;
    end;
    459: begin
      First := 9545;
      Last := 9550;
      result := true;
    end;
    460: begin
      First := 9551;
      Last := 9585;
      result := true;
    end;
    461: begin
      First := 9586;
      Last := 9604;
      result := true;
    end;
    462: begin
      First := 9605;
      Last := 9610;
      result := true;
    end;
    463: begin
      First := 9611;
      Last := 9616;
      result := true;
    end;
    464: begin
      First := 9617;
      Last := 9622;
      result := true;
    end;
    465: begin
      First := 9623;
      Last := 9628;
      result := true;
    end;
    466: begin
      First := 9629;
      Last := 9641;
      result := true;
    end;
    467: begin
      First := 9642;
      Last := 9647;
      result := true;
    end;
    468: begin
      First := 9648;
      Last := 9665;
      result := true;
    end;
    469: begin
      First := 9666;
      Last := 9680;
      result := true;
    end;
    470: begin
      First := 9681;
      Last := 9686;
      result := true;
    end;
    471: begin
      First := 9687;
      Last := 9692;
      result := true;
    end;
    472: begin
      First := 9693;
      Last := 9698;
      result := true;
    end;
    473: begin
      First := 9699;
      Last := 9708;
      result := true;
    end;
    474: begin
      First := 9709;
      Last := 9714;
      result := true;
    end;
    475: begin
      First := 9715;
      Last := 9721;
      result := true;
    end;
    476: begin
      First := 9722;
      Last := 9727;
      result := true;
    end;
    477: begin
      First := 9728;
      Last := 9733;
      result := true;
    end;
    478: begin
      First := 9734;
      Last := 9739;
      result := true;
    end;
    479: begin
      First := 9740;
      Last := 9745;
      result := true;
    end;
    480: begin
      First := 9746;
      Last := 9751;
      result := true;
    end;
    481: begin
      First := 9752;
      Last := 9764;
      result := true;
    end;
    482: begin
      First := 9765;
      Last := 9773;
      result := true;
    end;
    483: begin
      First := 9774;
      Last := 9782;
      result := true;
    end;
    484: begin
      First := 9783;
      Last := 9788;
      result := true;
    end;
    485: begin
      First := 9789;
      Last := 9794;
      result := true;
    end;
    486: begin
      First := 9795;
      Last := 9800;
      result := true;
    end;
    487: begin
      First := 9801;
      Last := 9806;
      result := true;
    end;
    488: begin
      First := 9807;
      Last := 9812;
      result := true;
    end;
    489: begin
      First := 9813;
      Last := 9818;
      result := true;
    end;
    490: begin
      First := 9819;
      Last := 9841;
      result := true;
    end;
    491: begin
      First := 9842;
      Last := 9854;
      result := true;
    end;
    492: begin
      First := 9855;
      Last := 9869;
      result := true;
    end;
    493: begin
      First := 9870;
      Last := 9902;
      result := true;
    end;
    494: begin
      First := 9903;
      Last := 9946;
      result := true;
    end;
    495: begin
      First := 9947;
      Last := 9955;
      result := true;
    end;
    496: begin
      First := 9956;
      Last := 9991;
      result := true;
    end;
    497: begin
      First := 9992;
      Last := 9998;
      result := true;
    end;
    498: begin
      First := 9999;
      Last := 10009;
      result := true;
    end;
    499: begin
      First := 10010;
      Last := 10026;
      result := true;
    end;
    500: begin
      First := 10027;
      Last := 10061;
      result := true;
    end;
    501: begin
      First := 10062;
      Last := 10067;
      result := true;
    end;
    502: begin
      First := 10068;
      Last := 10073;
      result := true;
    end;
    503: begin
      First := 10074;
      Last := 10092;
      result := true;
    end;
    504: begin
      First := 10093;
      Last := 10099;
      result := true;
    end;
    505: begin
      First := 10100;
      Last := 10115;
      result := true;
    end;
    506: begin
      First := 10116;
      Last := 10129;
      result := true;
    end;
    507: begin
      First := 10130;
      Last := 10135;
      result := true;
    end;
    508: begin
      First := 10136;
      Last := 10141;
      result := true;
    end;
    509: begin
      First := 10142;
      Last := 10159;
      result := true;
    end;
    510: begin
      First := 10160;
      Last := 10166;
      result := true;
    end;
    511: begin
      First := 10167;
      Last := 10183;
      result := true;
    end;
    512: begin
      First := 10184;
      Last := 10192;
      result := true;
    end;
    513: begin
      First := 10193;
      Last := 10198;
      result := true;
    end;
    514: begin
      First := 10199;
      Last := 10204;
      result := true;
    end;
    515: begin
      First := 10205;
      Last := 10343;
      result := true;
    end;
    516: begin
      First := 10344;
      Last := 10435;
      result := true;
    end;
    517: begin
      First := 10436;
      Last := 10444;
      result := true;
    end;
    518: begin
      First := 10445;
      Last := 10450;
      result := true;
    end;
    519: begin
      First := 10451;
      Last := 10456;
      result := true;
    end;
    520: begin
      First := 10457;
      Last := 10548;
      result := true;
    end;
    521: begin
      First := 10549;
      Last := 10641;
      result := true;
    end;
    522: begin
      First := 10642;
      Last := 10647;
      result := true;
    end;
    523: begin
      First := 10648;
      Last := 10653;
      result := true;
    end;
    524: begin
      First := 10654;
      Last := 10659;
      result := true;
    end;
    525: begin
      First := 10660;
      Last := 10666;
      result := true;
    end;
    526: begin
      First := 10667;
      Last := 10675;
      result := true;
    end;
    527: begin
      First := 10676;
      Last := 10681;
      result := true;
    end;
    528: begin
      First := 10682;
      Last := 10697;
      result := true;
    end;
    529: begin
      First := 10698;
      Last := 10704;
      result := true;
    end;
    530: begin
      First := 10705;
      Last := 10724;
      result := true;
    end;
    531: begin
      First := 10725;
      Last := 10731;
      result := true;
    end;
    532: begin
      First := 10732;
      Last := 10740;
      result := true;
    end;
    533: begin
      First := 10741;
      Last := 10748;
      result := true;
    end;
    534: begin
      First := 10749;
      Last := 10754;
      result := true;
    end;
    535: begin
      First := 10755;
      Last := 10760;
      result := true;
    end;
    536: begin
      First := 10761;
      Last := 10766;
      result := true;
    end;
    537: begin
      First := 10767;
      Last := 10772;
      result := true;
    end;
    538: begin
      First := 10773;
      Last := 10778;
      result := true;
    end;
    539: begin
      First := 10779;
      Last := 10784;
      result := true;
    end;
    540: begin
      First := 10785;
      Last := 10790;
      result := true;
    end;
    541: begin
      First := 10791;
      Last := 10796;
      result := true;
    end;
    542: begin
      First := 10797;
      Last := 10814;
      result := true;
    end;
    543: begin
      First := 10815;
      Last := 10821;
      result := true;
    end;
    544: begin
      First := 10822;
      Last := 10828;
      result := true;
    end;
    545: begin
      First := 10829;
      Last := 10844;
      result := true;
    end;
    546: begin
      First := 10845;
      Last := 10860;
      result := true;
    end;
    547: begin
      First := 10861;
      Last := 10866;
      result := true;
    end;
    548: begin
      First := 10867;
      Last := 10882;
      result := true;
    end;
    549: begin
      First := 10883;
      Last := 10889;
      result := true;
    end;
    550: begin
      First := 10890;
      Last := 10898;
      result := true;
    end;
    551: begin
      First := 10899;
      Last := 10904;
      result := true;
    end;
    552: begin
      First := 10905;
      Last := 10910;
      result := true;
    end;
    553: begin
      First := 10911;
      Last := 10943;
      result := true;
    end;
    554: begin
      First := 10944;
      Last := 10974;
      result := true;
    end;
    555: begin
      First := 10975;
      Last := 10981;
      result := true;
    end;
    556: begin
      First := 10982;
      Last := 10990;
      result := true;
    end;
    557: begin
      First := 10991;
      Last := 11009;
      result := true;
    end;
    558: begin
      First := 11010;
      Last := 11015;
      result := true;
    end;
    559: begin
      First := 11016;
      Last := 11136;
      result := true;
    end;
    560: begin
      First := 11137;
      Last := 11179;
      result := true;
    end;
    561: begin
      First := 11180;
      Last := 11185;
      result := true;
    end;
    562: begin
      First := 11186;
      Last := 11212;
      result := true;
    end;
    563: begin
      First := 11213;
      Last := 11218;
      result := true;
    end;
    564: begin
      First := 11219;
      Last := 11224;
      result := true;
    end;
    565: begin
      First := 11225;
      Last := 11239;
      result := true;
    end;
    566: begin
      First := 11240;
      Last := 11258;
      result := true;
    end;
    567: begin
      First := 11259;
      Last := 11271;
      result := true;
    end;
    568: begin
      First := 11272;
      Last := 11287;
      result := true;
    end;
    569: begin
      First := 11288;
      Last := 11293;
      result := true;
    end;
    570: begin
      First := 11294;
      Last := 11299;
      result := true;
    end;
    571: begin
      First := 11300;
      Last := 11305;
      result := true;
    end;
    572: begin
      First := 11306;
      Last := 11311;
      result := true;
    end;
    573: begin
      First := 11312;
      Last := 11346;
      result := true;
    end;
    574: begin
      First := 11347;
      Last := 11352;
      result := true;
    end;
    575: begin
      First := 11353;
      Last := 11366;
      result := true;
    end;
    576: begin
      First := 11367;
      Last := 11410;
      result := true;
    end;
    577: begin
      First := 11411;
      Last := 11422;
      result := true;
    end;
    578: begin
      First := 11423;
      Last := 11428;
      result := true;
    end;
    579: begin
      First := 11429;
      Last := 11434;
      result := true;
    end;
    580: begin
      First := 11435;
      Last := 11560;
      result := true;
    end;
    581: begin
      First := 11561;
      Last := 11566;
      result := true;
    end;
    582: begin
      First := 11567;
      Last := 11572;
      result := true;
    end;
    583: begin
      First := 11573;
      Last := 11579;
      result := true;
    end;
    584: begin
      First := 11580;
      Last := 11585;
      result := true;
    end;
    585: begin
      First := 11586;
      Last := 11620;
      result := true;
    end;
    586: begin
      First := 11621;
      Last := 11626;
      result := true;
    end;
    587: begin
      First := 11627;
      Last := 11639;
      result := true;
    end;
    588: begin
      First := 11640;
      Last := 11645;
      result := true;
    end;
    589: begin
      First := 11646;
      Last := 11763;
      result := true;
    end;
    590: begin
      First := 11764;
      Last := 11770;
      result := true;
    end;
    591: begin
      First := 11771;
      Last := 11776;
      result := true;
    end;
    592: begin
      First := 11777;
      Last := 11782;
      result := true;
    end;
    593: begin
      First := 11783;
      Last := 11788;
      result := true;
    end;
    594: begin
      First := 11789;
      Last := 11794;
      result := true;
    end;
    595: begin
      First := 11795;
      Last := 11804;
      result := true;
    end;
    596: begin
      First := 11805;
      Last := 11819;
      result := true;
    end;
    597: begin
      First := 11820;
      Last := 11825;
      result := true;
    end;
    598: begin
      First := 11826;
      Last := 11831;
      result := true;
    end;
    599: begin
      First := 11832;
      Last := 11838;
      result := true;
    end;
    600: begin
      First := 11839;
      Last := 11844;
      result := true;
    end;
    601: begin
      First := 11845;
      Last := 11850;
      result := true;
    end;
    602: begin
      First := 11851;
      Last := 11860;
      result := true;
    end;
    603: begin
      First := 11861;
      Last := 11938;
      result := true;
    end;
    604: begin
      First := 11939;
      Last := 12016;
      result := true;
    end;
    605: begin
      First := 12017;
      Last := 12094;
      result := true;
    end;
    606: begin
      First := 12095;
      Last := 12100;
      result := true;
    end;
    607: begin
      First := 12101;
      Last := 12106;
      result := true;
    end;
    608: begin
      First := 12107;
      Last := 12224;
      result := true;
    end;
    609: begin
      First := 12225;
      Last := 12230;
      result := true;
    end;
    610: begin
      First := 12231;
      Last := 12246;
      result := true;
    end;
    611: begin
      First := 12247;
      Last := 12259;
      result := true;
    end;
    612: begin
      First := 12260;
      Last := 12265;
      result := true;
    end;
    613: begin
      First := 12266;
      Last := 12272;
      result := true;
    end;
    614: begin
      First := 12273;
      Last := 12391;
      result := true;
    end;
    615: begin
      First := 12392;
      Last := 12403;
      result := true;
    end;
    616: begin
      First := 12404;
      Last := 12522;
      result := true;
    end;
    617: begin
      First := 12523;
      Last := 12529;
      result := true;
    end;
    618: begin
      First := 12530;
      Last := 12560;
      result := true;
    end;
    619: begin
      First := 12561;
      Last := 12589;
      result := true;
    end;
    620: begin
      First := 12590;
      Last := 12595;
      result := true;
    end;
    621: begin
      First := 12596;
      Last := 12601;
      result := true;
    end;
    622: begin
      First := 12602;
      Last := 12607;
      result := true;
    end;
    623: begin
      First := 12608;
      Last := 12613;
      result := true;
    end;
    624: begin
      First := 12614;
      Last := 12619;
      result := true;
    end;
    625: begin
      First := 12620;
      Last := 12625;
      result := true;
    end;
    626: begin
      First := 12626;
      Last := 12631;
      result := true;
    end;
    627: begin
      First := 12632;
      Last := 12637;
      result := true;
    end;
    628: begin
      First := 12638;
      Last := 12643;
      result := true;
    end;
    629: begin
      First := 12644;
      Last := 12649;
      result := true;
    end;
    630: begin
      First := 12650;
      Last := 12655;
      result := true;
    end;
    631: begin
      First := 12656;
      Last := 12708;
      result := true;
    end;
    632: begin
      First := 12709;
      Last := 12714;
      result := true;
    end;
    633: begin
      First := 12715;
      Last := 12720;
      result := true;
    end;
    634: begin
      First := 12721;
      Last := 12771;
      result := true;
    end;
    635: begin
      First := 12772;
      Last := 12780;
      result := true;
    end;
    636: begin
      First := 12781;
      Last := 12936;
      result := true;
    end;
    637: begin
      First := 12937;
      Last := 12945;
      result := true;
    end;
    638: begin
      First := 12946;
      Last := 13080;
      result := true;
    end;
    639: begin
      First := 13081;
      Last := 13089;
      result := true;
    end;
    640: begin
      First := 13090;
      Last := 13096;
      result := true;
    end;
    641: begin
      First := 13097;
      Last := 13109;
      result := true;
    end;
    642: begin
      First := 13110;
      Last := 13118;
      result := true;
    end;
    643: begin
      First := 13119;
      Last := 13132;
      result := true;
    end;
    644: begin
      First := 13133;
      Last := 13145;
      result := true;
    end;
    645: begin
      First := 13146;
      Last := 13151;
      result := true;
    end;
    646: begin
      First := 13152;
      Last := 13158;
      result := true;
    end;
    647: begin
      First := 13159;
      Last := 13167;
      result := true;
    end;
    648: begin
      First := 13168;
      Last := 13173;
      result := true;
    end;
    649: begin
      First := 13174;
      Last := 13180;
      result := true;
    end;
    650: begin
      First := 13181;
      Last := 13186;
      result := true;
    end;
    651: begin
      First := 13187;
      Last := 13192;
      result := true;
    end;
    652: begin
      First := 13193;
      Last := 13199;
      result := true;
    end;
    653: begin
      First := 13200;
      Last := 13205;
      result := true;
    end;
    654: begin
      First := 13206;
      Last := 13211;
      result := true;
    end;
    655: begin
      First := 13212;
      Last := 13217;
      result := true;
    end;
    656: begin
      First := 13218;
      Last := 13223;
      result := true;
    end;
    657: begin
      First := 13224;
      Last := 13236;
      result := true;
    end;
    658: begin
      First := 13237;
      Last := 13249;
      result := true;
    end;
    659: begin
      First := 13250;
      Last := 13262;
      result := true;
    end;
    660: begin
      First := 13263;
      Last := 13275;
      result := true;
    end;
    661: begin
      First := 13276;
      Last := 13288;
      result := true;
    end;
    662: begin
      First := 13289;
      Last := 13301;
      result := true;
    end;
    663: begin
      First := 13302;
      Last := 13308;
      result := true;
    end;
    664: begin
      First := 13309;
      Last := 13429;
      result := true;
    end;
    665: begin
      First := 13430;
      Last := 13435;
      result := true;
    end;
    666: begin
      First := 13436;
      Last := 13442;
      result := true;
    end;
    667: begin
      First := 13443;
      Last := 13457;
      result := true;
    end;
    668: begin
      First := 13458;
      Last := 13463;
      result := true;
    end;
    669: begin
      First := 13464;
      Last := 13469;
      result := true;
    end;
    670: begin
      First := 13470;
      Last := 13475;
      result := true;
    end;
    671: begin
      First := 13476;
      Last := 13488;
      result := true;
    end;
    672: begin
      First := 13489;
      Last := 13494;
      result := true;
    end;
    673: begin
      First := 13495;
      Last := 13500;
      result := true;
    end;
    674: begin
      First := 13501;
      Last := 13506;
      result := true;
    end;
    675: begin
      First := 13507;
      Last := 13513;
      result := true;
    end;
    676: begin
      First := 13514;
      Last := 13550;
      result := true;
    end;
    677: begin
      First := 13551;
      Last := 13556;
      result := true;
    end;
    678: begin
      First := 13557;
      Last := 13562;
      result := true;
    end;
    679: begin
      First := 13563;
      Last := 13597;
      result := true;
    end;
    680: begin
      First := 13598;
      Last := 13616;
      result := true;
    end;
    681: begin
      First := 13617;
      Last := 13653;
      result := true;
    end;
    682: begin
      First := 13654;
      Last := 13659;
      result := true;
    end;
    683: begin
      First := 13660;
      Last := 13675;
      result := true;
    end;
    684: begin
      First := 13676;
      Last := 13688;
      result := true;
    end;
    685: begin
      First := 13689;
      Last := 13694;
      result := true;
    end;
    686: begin
      First := 13695;
      Last := 13703;
      result := true;
    end;
    687: begin
      First := 13704;
      Last := 13709;
      result := true;
    end;
    688: begin
      First := 13710;
      Last := 13725;
      result := true;
    end;
    689: begin
      First := 13726;
      Last := 13735;
      result := true;
    end;
    690: begin
      First := 13736;
      Last := 13741;
      result := true;
    end;
    691: begin
      First := 13742;
      Last := 13747;
      result := true;
    end;
    692: begin
      First := 13748;
      Last := 13756;
      result := true;
    end;
    693: begin
      First := 13757;
      Last := 13772;
      result := true;
    end;
    694: begin
      First := 13773;
      Last := 13788;
      result := true;
    end;
    695: begin
      First := 13789;
      Last := 13794;
      result := true;
    end;
    696: begin
      First := 13795;
      Last := 13800;
      result := true;
    end;
    697: begin
      First := 13801;
      Last := 13853;
      result := true;
    end;
    698: begin
      First := 13854;
      Last := 13869;
      result := true;
    end;
    699: begin
      First := 13870;
      Last := 13885;
      result := true;
    end;
    700: begin
      First := 13886;
      Last := 13891;
      result := true;
    end;
    701: begin
      First := 13892;
      Last := 13897;
      result := true;
    end;
    702: begin
      First := 13898;
      Last := 13912;
      result := true;
    end;
    703: begin
      First := 13913;
      Last := 13927;
      result := true;
    end;
    704: begin
      First := 13928;
      Last := 13943;
      result := true;
    end;
    705: begin
      First := 13944;
      Last := 13949;
      result := true;
    end;
    706: begin
      First := 13950;
      Last := 13993;
      result := true;
    end;
    707: begin
      First := 13994;
      Last := 13999;
      result := true;
    end;
    708: begin
      First := 14000;
      Last := 14032;
      result := true;
    end;
    709: begin
      First := 14033;
      Last := 14058;
      result := true;
    end;
    710: begin
      First := 14059;
      Last := 14095;
      result := true;
    end;
    711: begin
      First := 14096;
      Last := 14101;
      result := true;
    end;
    712: begin
      First := 14102;
      Last := 14107;
      result := true;
    end;
    713: begin
      First := 14108;
      Last := 14144;
      result := true;
    end;
    714: begin
      First := 14145;
      Last := 14153;
      result := true;
    end;
    715: begin
      First := 14154;
      Last := 14292;
      result := true;
    end;
    716: begin
      First := 14293;
      Last := 14301;
      result := true;
    end;
    717: begin
      First := 14302;
      Last := 14308;
      result := true;
    end;
    718: begin
      First := 14309;
      Last := 14314;
      result := true;
    end;
    719: begin
      First := 14315;
      Last := 14320;
      result := true;
    end;
    720: begin
      First := 14321;
      Last := 14326;
      result := true;
    end;
    721: begin
      First := 14327;
      Last := 14332;
      result := true;
    end;
    722: begin
      First := 14333;
      Last := 14338;
      result := true;
    end;
    723: begin
      First := 14339;
      Last := 14344;
      result := true;
    end;
    724: begin
      First := 14345;
      Last := 14350;
      result := true;
    end;
    725: begin
      First := 14351;
      Last := 14393;
      result := true;
    end;
    726: begin
      First := 14394;
      Last := 14430;
      result := true;
    end;
    727: begin
      First := 14431;
      Last := 14479;
      result := true;
    end;
    728: begin
      First := 14480;
      Last := 14529;
      result := true;
    end;
    729: begin
      First := 14530;
      Last := 14579;
      result := true;
    end;
    730: begin
      First := 14580;
      Last := 14585;
      result := true;
    end;
    731: begin
      First := 14586;
      Last := 14631;
      result := true;
    end;
    732: begin
      First := 14632;
      Last := 14677;
      result := true;
    end;
    733: begin
      First := 14678;
      Last := 14684;
      result := true;
    end;
    734: begin
      First := 14685;
      Last := 14728;
      result := true;
    end;
    735: begin
      First := 14729;
      Last := 14734;
      result := true;
    end;
    736: begin
      First := 14735;
      Last := 14740;
      result := true;
    end;
    737: begin
      First := 14741;
      Last := 14756;
      result := true;
    end;
    738: begin
      First := 14757;
      Last := 14771;
      result := true;
    end;
    739: begin
      First := 14772;
      Last := 14820;
      result := true;
    end;
    740: begin
      First := 14821;
      Last := 14866;
      result := true;
    end;
    741: begin
      First := 14867;
      Last := 14872;
      result := true;
    end;
    742: begin
      First := 14873;
      Last := 14878;
      result := true;
    end;
    743: begin
      First := 14879;
      Last := 14925;
      result := true;
    end;
    744: begin
      First := 14926;
      Last := 14972;
      result := true;
    end;
    745: begin
      First := 14973;
      Last := 15019;
      result := true;
    end;
    746: begin
      First := 15020;
      Last := 15025;
      result := true;
    end;
    747: begin
      First := 15026;
      Last := 15070;
      result := true;
    end;
    748: begin
      First := 15071;
      Last := 15076;
      result := true;
    end;
    749: begin
      First := 15077;
      Last := 15082;
      result := true;
    end;
    750: begin
      First := 15083;
      Last := 15088;
      result := true;
    end;
    751: begin
      First := 15089;
      Last := 15094;
      result := true;
    end;
    752: begin
      First := 15095;
      Last := 15100;
      result := true;
    end;
    753: begin
      First := 15101;
      Last := 15106;
      result := true;
    end;
    754: begin
      First := 15107;
      Last := 15112;
      result := true;
    end;
    755: begin
      First := 15113;
      Last := 15118;
      result := true;
    end;
    756: begin
      First := 15119;
      Last := 15142;
      result := true;
    end;
    757: begin
      First := 15143;
      Last := 15149;
      result := true;
    end;
    758: begin
      First := 15150;
      Last := 15155;
      result := true;
    end;
    759: begin
      First := 15156;
      Last := 15161;
      result := true;
    end;
    760: begin
      First := 15162;
      Last := 15168;
      result := true;
    end;
    761: begin
      First := 15169;
      Last := 15174;
      result := true;
    end;
    762: begin
      First := 15175;
      Last := 15194;
      result := true;
    end;
    763: begin
      First := 15195;
      Last := 15201;
      result := true;
    end;
    764: begin
      First := 15202;
      Last := 15210;
      result := true;
    end;
    765: begin
      First := 15211;
      Last := 15219;
      result := true;
    end;
    766: begin
      First := 15220;
      Last := 15259;
      result := true;
    end;
    767: begin
      First := 15260;
      Last := 15351;
      result := true;
    end;
    768: begin
      First := 15352;
      Last := 15357;
      result := true;
    end;
    769: begin
      First := 15358;
      Last := 15366;
      result := true;
    end;
    770: begin
      First := 15367;
      Last := 15372;
      result := true;
    end;
    771: begin
      First := 15373;
      Last := 15378;
      result := true;
    end;
    772: begin
      First := 15379;
      Last := 15389;
      result := true;
    end;
    773: begin
      First := 15390;
      Last := 15398;
      result := true;
    end;
    774: begin
      First := 15399;
      Last := 15407;
      result := true;
    end;
    775: begin
      First := 15408;
      Last := 15418;
      result := true;
    end;
    776: begin
      First := 15419;
      Last := 15425;
      result := true;
    end;
    777: begin
      First := 15426;
      Last := 15434;
      result := true;
    end;
    778: begin
      First := 15435;
      Last := 15445;
      result := true;
    end;
    779: begin
      First := 15446;
      Last := 15451;
      result := true;
    end;
    780: begin
      First := 15452;
      Last := 15469;
      result := true;
    end;
    781: begin
      First := 15470;
      Last := 15485;
      result := true;
    end;
    782: begin
      First := 15486;
      Last := 15491;
      result := true;
    end;
    783: begin
      First := 15492;
      Last := 15497;
      result := true;
    end;
    784: begin
      First := 15498;
      Last := 15503;
      result := true;
    end;
    785: begin
      First := 15504;
      Last := 15509;
      result := true;
    end;
    786: begin
      First := 15510;
      Last := 15524;
      result := true;
    end;
    787: begin
      First := 15525;
      Last := 15531;
      result := true;
    end;
    788: begin
      First := 15532;
      Last := 15540;
      result := true;
    end;
    789: begin
      First := 15541;
      Last := 15546;
      result := true;
    end;
    790: begin
      First := 15547;
      Last := 15553;
      result := true;
    end;
    791: begin
      First := 15554;
      Last := 15560;
      result := true;
    end;
    792: begin
      First := 15561;
      Last := 15567;
      result := true;
    end;
    793: begin
      First := 15568;
      Last := 15573;
      result := true;
    end;
    794: begin
      First := 15574;
      Last := 15615;
      result := true;
    end;
    795: begin
      First := 15616;
      Last := 15621;
      result := true;
    end;
    796: begin
      First := 15622;
      Last := 15627;
      result := true;
    end;
    797: begin
      First := 15628;
      Last := 15645;
      result := true;
    end;
    798: begin
      First := 15646;
      Last := 15651;
      result := true;
    end;
    799: begin
      First := 15652;
      Last := 15657;
      result := true;
    end;
    800: begin
      First := 15658;
      Last := 15716;
      result := true;
    end;
    801: begin
      First := 15717;
      Last := 15757;
      result := true;
    end;
    802: begin
      First := 15758;
      Last := 15763;
      result := true;
    end;
    803: begin
      First := 15764;
      Last := 15828;
      result := true;
    end;
    804: begin
      First := 15829;
      Last := 15866;
      result := true;
    end;
    805: begin
      First := 15867;
      Last := 15872;
      result := true;
    end;
    806: begin
      First := 15873;
      Last := 15902;
      result := true;
    end;
    807: begin
      First := 15903;
      Last := 15908;
      result := true;
    end;
    808: begin
      First := 15909;
      Last := 15914;
      result := true;
    end;
    809: begin
      First := 15915;
      Last := 15920;
      result := true;
    end;
    810: begin
      First := 15921;
      Last := 15926;
      result := true;
    end;
    811: begin
      First := 15927;
      Last := 15932;
      result := true;
    end;
    812: begin
      First := 15933;
      Last := 15938;
      result := true;
    end;
    813: begin
      First := 15939;
      Last := 15947;
      result := true;
    end;
    814: begin
      First := 15948;
      Last := 15954;
      result := true;
    end;
    815: begin
      First := 15955;
      Last := 15960;
      result := true;
    end;
    816: begin
      First := 15961;
      Last := 15966;
      result := true;
    end;
    817: begin
      First := 15967;
      Last := 15980;
      result := true;
    end;
    818: begin
      First := 15981;
      Last := 16099;
      result := true;
    end;
    819: begin
      First := 16100;
      Last := 16105;
      result := true;
    end;
    820: begin
      First := 16106;
      Last := 16122;
      result := true;
    end;
    821: begin
      First := 16123;
      Last := 16128;
      result := true;
    end;
    822: begin
      First := 16129;
      Last := 16141;
      result := true;
    end;
    823: begin
      First := 16142;
      Last := 16147;
      result := true;
    end;
    824: begin
      First := 16148;
      Last := 16154;
      result := true;
    end;
    825: begin
      First := 16155;
      Last := 16164;
      result := true;
    end;
    826: begin
      First := 16165;
      Last := 16170;
      result := true;
    end;
    827: begin
      First := 16171;
      Last := 16176;
      result := true;
    end;
    828: begin
      First := 16177;
      Last := 16272;
      result := true;
    end;
    829: begin
      First := 16273;
      Last := 16390;
      result := true;
    end;
    830: begin
      First := 16391;
      Last := 16401;
      result := true;
    end;
    831: begin
      First := 16402;
      Last := 16411;
      result := true;
    end;
    832: begin
      First := 16412;
      Last := 16425;
      result := true;
    end;
    833: begin
      First := 16426;
      Last := 16431;
      result := true;
    end;
    834: begin
      First := 16432;
      Last := 16437;
      result := true;
    end;
    835: begin
      First := 16438;
      Last := 16443;
      result := true;
    end;
    836: begin
      First := 16444;
      Last := 16457;
      result := true;
    end;
    837: begin
      First := 16458;
      Last := 16464;
      result := true;
    end;
    838: begin
      First := 16465;
      Last := 16471;
      result := true;
    end;
    839: begin
      First := 16472;
      Last := 16484;
      result := true;
    end;
    840: begin
      First := 16485;
      Last := 16604;
      result := true;
    end;
    841: begin
      First := 16605;
      Last := 16723;
      result := true;
    end;
    842: begin
      First := 16724;
      Last := 16730;
      result := true;
    end;
    843: begin
      First := 16731;
      Last := 16856;
      result := true;
    end;
    844: begin
      First := 16857;
      Last := 16862;
      result := true;
    end;
    845: begin
      First := 16863;
      Last := 16868;
      result := true;
    end;
    846: begin
      First := 16869;
      Last := 16874;
      result := true;
    end;
    847: begin
      First := 16875;
      Last := 16880;
      result := true;
    end;
    848: begin
      First := 16881;
      Last := 16886;
      result := true;
    end;
    849: begin
      First := 16887;
      Last := 16892;
      result := true;
    end;
    850: begin
      First := 16893;
      Last := 17011;
      result := true;
    end;
    851: begin
      First := 17012;
      Last := 17021;
      result := true;
    end;
    852: begin
      First := 17022;
      Last := 17030;
      result := true;
    end;
    853: begin
      First := 17031;
      Last := 17043;
      result := true;
    end;
    854: begin
      First := 17044;
      Last := 17052;
      result := true;
    end;
    855: begin
      First := 17053;
      Last := 17065;
      result := true;
    end;
    856: begin
      First := 17066;
      Last := 17202;
      result := true;
    end;
    857: begin
      First := 17203;
      Last := 17340;
      result := true;
    end;
    858: begin
      First := 17341;
      Last := 17459;
      result := true;
    end;
    859: begin
      First := 17460;
      Last := 17495;
      result := true;
    end;
    860: begin
      First := 17496;
      Last := 17504;
      result := true;
    end;
    861: begin
      First := 17505;
      Last := 17661;
      result := true;
    end;
    862: begin
      First := 17662;
      Last := 17667;
      result := true;
    end;
    863: begin
      First := 17668;
      Last := 17673;
      result := true;
    end;
    864: begin
      First := 17674;
      Last := 17679;
      result := true;
    end;
    865: begin
      First := 17680;
      Last := 17724;
      result := true;
    end;
    866: begin
      First := 17725;
      Last := 17730;
      result := true;
    end;
    867: begin
      First := 17731;
      Last := 17848;
      result := true;
    end;
    868: begin
      First := 17849;
      Last := 17864;
      result := true;
    end;
    869: begin
      First := 17865;
      Last := 17870;
      result := true;
    end;
    870: begin
      First := 17871;
      Last := 17876;
      result := true;
    end;
    871: begin
      First := 17877;
      Last := 17995;
      result := true;
    end;
    872: begin
      First := 17996;
      Last := 18001;
      result := true;
    end;
    873: begin
      First := 18002;
      Last := 18119;
      result := true;
    end;
    874: begin
      First := 18120;
      Last := 18125;
      result := true;
    end;
    875: begin
      First := 18126;
      Last := 18243;
      result := true;
    end;
    876: begin
      First := 18244;
      Last := 18362;
      result := true;
    end;
    877: begin
      First := 18363;
      Last := 18378;
      result := true;
    end;
    878: begin
      First := 18379;
      Last := 18497;
      result := true;
    end;
    879: begin
      First := 18498;
      Last := 18504;
      result := true;
    end;
    880: begin
      First := 18505;
      Last := 18517;
      result := true;
    end;
    881: begin
      First := 18518;
      Last := 18636;
      result := true;
    end;
    882: begin
      First := 18637;
      Last := 18642;
      result := true;
    end;
    883: begin
      First := 18643;
      Last := 18648;
      result := true;
    end;
    884: begin
      First := 18649;
      Last := 18665;
      result := true;
    end;
    885: begin
      First := 18666;
      Last := 18671;
      result := true;
    end;
    886: begin
      First := 18672;
      Last := 18677;
      result := true;
    end;
    887: begin
      First := 18678;
      Last := 18683;
      result := true;
    end;
    888: begin
      First := 18684;
      Last := 18689;
      result := true;
    end;
    889: begin
      First := 18690;
      Last := 18701;
      result := true;
    end;
    890: begin
      First := 18702;
      Last := 18707;
      result := true;
    end;
    891: begin
      First := 18708;
      Last := 18714;
      result := true;
    end;
    892: begin
      First := 18715;
      Last := 18728;
      result := true;
    end;
    893: begin
      First := 18729;
      Last := 18734;
      result := true;
    end;
    894: begin
      First := 18735;
      Last := 18740;
      result := true;
    end;
    895: begin
      First := 18741;
      Last := 18746;
      result := true;
    end;
    896: begin
      First := 18747;
      Last := 18783;
      result := true;
    end;
    897: begin
      First := 18784;
      Last := 18790;
      result := true;
    end;
    898: begin
      First := 18791;
      Last := 18796;
      result := true;
    end;
    899: begin
      First := 18797;
      Last := 18802;
      result := true;
    end;
    900: begin
      First := 18803;
      Last := 18808;
      result := true;
    end;
    901: begin
      First := 18809;
      Last := 18814;
      result := true;
    end;
    902: begin
      First := 18815;
      Last := 18820;
      result := true;
    end;
    903: begin
      First := 18821;
      Last := 18826;
      result := true;
    end;
    904: begin
      First := 18827;
      Last := 18832;
      result := true;
    end;
    905: begin
      First := 18833;
      Last := 18838;
      result := true;
    end;
    906: begin
      First := 18839;
      Last := 18844;
      result := true;
    end;
    907: begin
      First := 18845;
      Last := 18850;
      result := true;
    end;
    908: begin
      First := 18851;
      Last := 18895;
      result := true;
    end;
    909: begin
      First := 18896;
      Last := 18901;
      result := true;
    end;
    910: begin
      First := 18902;
      Last := 18907;
      result := true;
    end;
    911: begin
      First := 18908;
      Last := 18913;
      result := true;
    end;
    912: begin
      First := 18914;
      Last := 18949;
      result := true;
    end;
    913: begin
      First := 18950;
      Last := 18955;
      result := true;
    end;
    914: begin
      First := 18956;
      Last := 18962;
      result := true;
    end;
    915: begin
      First := 18963;
      Last := 18969;
      result := true;
    end;
    916: begin
      First := 18970;
      Last := 18976;
      result := true;
    end;
    917: begin
      First := 18977;
      Last := 18982;
      result := true;
    end;
    918: begin
      First := 18983;
      Last := 18994;
      result := true;
    end;
    919: begin
      First := 18995;
      Last := 19004;
      result := true;
    end;
    920: begin
      First := 19005;
      Last := 19014;
      result := true;
    end;
    921: begin
      First := 19015;
      Last := 19020;
      result := true;
    end;
    922: begin
      First := 19021;
      Last := 19026;
      result := true;
    end;
    923: begin
      First := 19027;
      Last := 19032;
      result := true;
    end;
    924: begin
      First := 19033;
      Last := 19040;
      result := true;
    end;
    925: begin
      First := 19041;
      Last := 19046;
      result := true;
    end;
    926: begin
      First := 19047;
      Last := 19052;
      result := true;
    end;
    927: begin
      First := 19053;
      Last := 19061;
      result := true;
    end;
    928: begin
      First := 19062;
      Last := 19067;
      result := true;
    end;
    929: begin
      First := 19068;
      Last := 19073;
      result := true;
    end;
    930: begin
      First := 19074;
      Last := 19080;
      result := true;
    end;
    931: begin
      First := 19081;
      Last := 19086;
      result := true;
    end;
    932: begin
      First := 19087;
      Last := 19092;
      result := true;
    end;
    933: begin
      First := 19093;
      Last := 19098;
      result := true;
    end;
    934: begin
      First := 19099;
      Last := 19104;
      result := true;
    end;
    935: begin
      First := 19105;
      Last := 19113;
      result := true;
    end;
    936: begin
      First := 19114;
      Last := 19149;
      result := true;
    end;
    937: begin
      First := 19150;
      Last := 19166;
      result := true;
    end;
    938: begin
      First := 19167;
      Last := 19191;
      result := true;
    end;
    939: begin
      First := 19192;
      Last := 19197;
      result := true;
    end;
    940: begin
      First := 19198;
      Last := 19204;
      result := true;
    end;
    941: begin
      First := 19205;
      Last := 19210;
      result := true;
    end;
    942: begin
      First := 19211;
      Last := 19249;
      result := true;
    end;
    943: begin
      First := 19250;
      Last := 19255;
      result := true;
    end;
    944: begin
      First := 19256;
      Last := 19265;
      result := true;
    end;
    945: begin
      First := 19266;
      Last := 19311;
      result := true;
    end;
    946: begin
      First := 19312;
      Last := 19317;
      result := true;
    end;
    947: begin
      First := 19318;
      Last := 19364;
      result := true;
    end;
    948: begin
      First := 19365;
      Last := 19370;
      result := true;
    end;
    949: begin
      First := 19371;
      Last := 19417;
      result := true;
    end;
    950: begin
      First := 19418;
      Last := 19423;
      result := true;
    end;
    951: begin
      First := 19424;
      Last := 19434;
      result := true;
    end;
    952: begin
      First := 19435;
      Last := 19440;
      result := true;
    end;
    953: begin
      First := 19441;
      Last := 19446;
      result := true;
    end;
    954: begin
      First := 19447;
      Last := 19456;
      result := true;
    end;
    955: begin
      First := 19457;
      Last := 19462;
      result := true;
    end;
    956: begin
      First := 19463;
      Last := 19511;
      result := true;
    end;
    957: begin
      First := 19512;
      Last := 19560;
      result := true;
    end;
    958: begin
      First := 19561;
      Last := 19566;
      result := true;
    end;
    959: begin
      First := 19567;
      Last := 19612;
      result := true;
    end;
    960: begin
      First := 19613;
      Last := 19618;
      result := true;
    end;
    961: begin
      First := 19619;
      Last := 19661;
      result := true;
    end;
    962: begin
      First := 19662;
      Last := 19672;
      result := true;
    end;
    963: begin
      First := 19673;
      Last := 19715;
      result := true;
    end;
    964: begin
      First := 19716;
      Last := 19726;
      result := true;
    end;
    965: begin
      First := 19727;
      Last := 19732;
      result := true;
    end;
    966: begin
      First := 19733;
      Last := 19738;
      result := true;
    end;
    967: begin
      First := 19739;
      Last := 19755;
      result := true;
    end;
    968: begin
      First := 19756;
      Last := 19773;
      result := true;
    end;
    969: begin
      First := 19774;
      Last := 19810;
      result := true;
    end;
    970: begin
      First := 19811;
      Last := 19816;
      result := true;
    end;
    971: begin
      First := 19817;
      Last := 19849;
      result := true;
    end;
    972: begin
      First := 19850;
      Last := 19858;
      result := true;
    end;
    973: begin
      First := 19859;
      Last := 19870;
      result := true;
    end;
    974: begin
      First := 19871;
      Last := 19879;
      result := true;
    end;
    975: begin
      First := 19880;
      Last := 19888;
      result := true;
    end;
    976: begin
      First := 19889;
      Last := 19894;
      result := true;
    end;
    977: begin
      First := 19895;
      Last := 19907;
      result := true;
    end;
    978: begin
      First := 19908;
      Last := 19917;
      result := true;
    end;
    979: begin
      First := 19918;
      Last := 19926;
      result := true;
    end;
    980: begin
      First := 19927;
      Last := 19939;
      result := true;
    end;
    981: begin
      First := 19940;
      Last := 19945;
      result := true;
    end;
    982: begin
      First := 19946;
      Last := 19980;
      result := true;
    end;
    983: begin
      First := 19981;
      Last := 19986;
      result := true;
    end;
    984: begin
      First := 19987;
      Last := 19992;
      result := true;
    end;
    985: begin
      First := 19993;
      Last := 19998;
      result := true;
    end;
    986: begin
      First := 19999;
      Last := 20004;
      result := true;
    end;
    987: begin
      First := 20005;
      Last := 20024;
      result := true;
    end;
    988: begin
      First := 20025;
      Last := 20039;
      result := true;
    end;
    989: begin
      First := 20040;
      Last := 20056;
      result := true;
    end;
    990: begin
      First := 20057;
      Last := 20074;
      result := true;
    end;
    991: begin
      First := 20075;
      Last := 20080;
      result := true;
    end;
    992: begin
      First := 20081;
      Last := 20121;
      result := true;
    end;
    993: begin
      First := 20122;
      Last := 20128;
      result := true;
    end;
    994: begin
      First := 20129;
      Last := 20134;
      result := true;
    end;
    995: begin
      First := 20135;
      Last := 20190;
      result := true;
    end;
    996: begin
      First := 20191;
      Last := 20196;
      result := true;
    end;
    997: begin
      First := 20197;
      Last := 20212;
      result := true;
    end;
    998: begin
      First := 20213;
      Last := 20219;
      result := true;
    end;
    999: begin
      First := 20220;
      Last := 20230;
      result := true;
    end;
    1000: begin
      First := 20231;
      Last := 20237;
      result := true;
    end;
    1001: begin
      First := 20238;
      Last := 20259;
      result := true;
    end;
    1002: begin
      First := 20260;
      Last := 20270;
      result := true;
    end;
    1003: begin
      First := 20271;
      Last := 20287;
      result := true;
    end;
    1004: begin
      First := 20288;
      Last := 20298;
      result := true;
    end;
    1005: begin
      First := 20299;
      Last := 20324;
      result := true;
    end;
    1006: begin
      First := 20325;
      Last := 20330;
      result := true;
    end;
    1007: begin
      First := 20331;
      Last := 20346;
      result := true;
    end;
    1008: begin
      First := 20347;
      Last := 20361;
      result := true;
    end;
    1009: begin
      First := 20362;
      Last := 20385;
      result := true;
    end;
    1010: begin
      First := 20386;
      Last := 20400;
      result := true;
    end;
    1011: begin
      First := 20401;
      Last := 20417;
      result := true;
    end;
    1012: begin
      First := 20418;
      Last := 20423;
      result := true;
    end;
    1013: begin
      First := 20424;
      Last := 20429;
      result := true;
    end;
    1014: begin
      First := 20430;
      Last := 20435;
      result := true;
    end;
    1015: begin
      First := 20436;
      Last := 20441;
      result := true;
    end;
    1016: begin
      First := 20442;
      Last := 20447;
      result := true;
    end;
    1017: begin
      First := 20448;
      Last := 20484;
      result := true;
    end;
    1018: begin
      First := 20485;
      Last := 20490;
      result := true;
    end;
    1019: begin
      First := 20491;
      Last := 20496;
      result := true;
    end;
    1020: begin
      First := 20497;
      Last := 20506;
      result := true;
    end;
    1021: begin
      First := 20507;
      Last := 20591;
      result := true;
    end;
    1022: begin
      First := 20592;
      Last := 20598;
      result := true;
    end;
    1023: begin
      First := 20599;
      Last := 20608;
      result := true;
    end;
    1024: begin
      First := 20609;
      Last := 20614;
      result := true;
    end;
    1025: begin
      First := 20615;
      Last := 20733;
      result := true;
    end;
    1026: begin
      First := 20734;
      Last := 20739;
      result := true;
    end;
    1027: begin
      First := 20740;
      Last := 20745;
      result := true;
    end;
    1028: begin
      First := 20746;
      Last := 20800;
      result := true;
    end;
    1029: begin
      First := 20801;
      Last := 20858;
      result := true;
    end;
    1030: begin
      First := 20859;
      Last := 20977;
      result := true;
    end;
    1031: begin
      First := 20978;
      Last := 20983;
      result := true;
    end;
    1032: begin
      First := 20984;
      Last := 20989;
      result := true;
    end;
    1033: begin
      First := 20990;
      Last := 20998;
      result := true;
    end;
    1034: begin
      First := 20999;
      Last := 21004;
      result := true;
    end;
    1035: begin
      First := 21005;
      Last := 21010;
      result := true;
    end;
    1036: begin
      First := 21011;
      Last := 21016;
      result := true;
    end;
    1037: begin
      First := 21017;
      Last := 21022;
      result := true;
    end;
    1038: begin
      First := 21023;
      Last := 21029;
      result := true;
    end;
    1039: begin
      First := 21030;
      Last := 21035;
      result := true;
    end;
    1040: begin
      First := 21036;
      Last := 21041;
      result := true;
    end;
    1041: begin
      First := 21042;
      Last := 21173;
      result := true;
    end;
    1042: begin
      First := 21174;
      Last := 21180;
      result := true;
    end;
    1043: begin
      First := 21181;
      Last := 21186;
      result := true;
    end;
    1044: begin
      First := 21187;
      Last := 21198;
      result := true;
    end;
    1045: begin
      First := 21199;
      Last := 21204;
      result := true;
    end;
    1046: begin
      First := 21205;
      Last := 21323;
      result := true;
    end;
    1047: begin
      First := 21324;
      Last := 21333;
      result := true;
    end;
    1048: begin
      First := 21334;
      Last := 21339;
      result := true;
    end;
    1049: begin
      First := 21340;
      Last := 21345;
      result := true;
    end;
    1050: begin
      First := 21346;
      Last := 21356;
      result := true;
    end;
    1051: begin
      First := 21357;
      Last := 21362;
      result := true;
    end;
    1052: begin
      First := 21363;
      Last := 21368;
      result := true;
    end;
    1053: begin
      First := 21369;
      Last := 21374;
      result := true;
    end;
    1054: begin
      First := 21375;
      Last := 21425;
      result := true;
    end;
    1055: begin
      First := 21426;
      Last := 21431;
      result := true;
    end;
    1056: begin
      First := 21432;
      Last := 21437;
      result := true;
    end;
    1057: begin
      First := 21438;
      Last := 21444;
      result := true;
    end;
    1058: begin
      First := 21445;
      Last := 21450;
      result := true;
    end;
    1059: begin
      First := 21451;
      Last := 21456;
      result := true;
    end;
    1060: begin
      First := 21457;
      Last := 21470;
      result := true;
    end;
    1061: begin
      First := 21471;
      Last := 21477;
      result := true;
    end;
    1062: begin
      First := 21478;
      Last := 21483;
      result := true;
    end;
    1063: begin
      First := 21484;
      Last := 21490;
      result := true;
    end;
    1064: begin
      First := 21491;
      Last := 21503;
      result := true;
    end;
    1065: begin
      First := 21504;
      Last := 21516;
      result := true;
    end;
    1066: begin
      First := 21517;
      Last := 21529;
      result := true;
    end;
    1067: begin
      First := 21530;
      Last := 21540;
      result := true;
    end;
    1068: begin
      First := 21541;
      Last := 21554;
      result := true;
    end;
    1069: begin
      First := 21555;
      Last := 21561;
      result := true;
    end;
    1070: begin
      First := 21562;
      Last := 21567;
      result := true;
    end;
    1071: begin
      First := 21568;
      Last := 21580;
      result := true;
    end;
    1072: begin
      First := 21581;
      Last := 21699;
      result := true;
    end;
    1073: begin
      First := 21700;
      Last := 21705;
      result := true;
    end;
    1074: begin
      First := 21706;
      Last := 21712;
      result := true;
    end;
    1075: begin
      First := 21713;
      Last := 21721;
      result := true;
    end;
    1076: begin
      First := 21722;
      Last := 21729;
      result := true;
    end;
    1077: begin
      First := 21730;
      Last := 21735;
      result := true;
    end;
    1078: begin
      First := 21736;
      Last := 21809;
      result := true;
    end;
    1079: begin
      First := 21810;
      Last := 21815;
      result := true;
    end;
    1080: begin
      First := 21816;
      Last := 21821;
      result := true;
    end;
    1081: begin
      First := 21822;
      Last := 21830;
      result := true;
    end;
    1082: begin
      First := 21831;
      Last := 21865;
      result := true;
    end;
    1083: begin
      First := 21866;
      Last := 21871;
      result := true;
    end;
    1084: begin
      First := 21872;
      Last := 21877;
      result := true;
    end;
    1085: begin
      First := 21878;
      Last := 21883;
      result := true;
    end;
    1086: begin
      First := 21884;
      Last := 22022;
      result := true;
    end;
    1087: begin
      First := 22023;
      Last := 22028;
      result := true;
    end;
    1088: begin
      First := 22029;
      Last := 22034;
      result := true;
    end;
    1089: begin
      First := 22035;
      Last := 22077;
      result := true;
    end;
    1090: begin
      First := 22078;
      Last := 22083;
      result := true;
    end;
    1091: begin
      First := 22084;
      Last := 22100;
      result := true;
    end;
    1092: begin
      First := 22101;
      Last := 22107;
      result := true;
    end;
    1093: begin
      First := 22108;
      Last := 22113;
      result := true;
    end;
    1094: begin
      First := 22114;
      Last := 22119;
      result := true;
    end;
    1095: begin
      First := 22120;
      Last := 22125;
      result := true;
    end;
    1096: begin
      First := 22126;
      Last := 22131;
      result := true;
    end;
    1097: begin
      First := 22132;
      Last := 22138;
      result := true;
    end;
    1098: begin
      First := 22139;
      Last := 22147;
      result := true;
    end;
    1099: begin
      First := 22148;
      Last := 22153;
      result := true;
    end;
    1100: begin
      First := 22154;
      Last := 22159;
      result := true;
    end;
    1101: begin
      First := 22160;
      Last := 22168;
      result := true;
    end;
    1102: begin
      First := 22169;
      Last := 22177;
      result := true;
    end;
    1103: begin
      First := 22178;
      Last := 22183;
      result := true;
    end;
    1104: begin
      First := 22184;
      Last := 22189;
      result := true;
    end;
    1105: begin
      First := 22190;
      Last := 22195;
      result := true;
    end;
    1106: begin
      First := 22196;
      Last := 22201;
      result := true;
    end;
    1107: begin
      First := 22202;
      Last := 22207;
      result := true;
    end;
    1108: begin
      First := 22208;
      Last := 22214;
      result := true;
    end;
    1109: begin
      First := 22215;
      Last := 22220;
      result := true;
    end;
    1110: begin
      First := 22221;
      Last := 22226;
      result := true;
    end;
    1111: begin
      First := 22227;
      Last := 22232;
      result := true;
    end;
    1112: begin
      First := 22233;
      Last := 22238;
      result := true;
    end;
    1113: begin
      First := 22239;
      Last := 22244;
      result := true;
    end;
    1114: begin
      First := 22245;
      Last := 22250;
      result := true;
    end;
    1115: begin
      First := 22251;
      Last := 22284;
      result := true;
    end;
    1116: begin
      First := 22285;
      Last := 22290;
      result := true;
    end;
    1117: begin
      First := 22291;
      Last := 22296;
      result := true;
    end;
    1118: begin
      First := 22297;
      Last := 22303;
      result := true;
    end;
    1119: begin
      First := 22304;
      Last := 22310;
      result := true;
    end;
    1120: begin
      First := 22311;
      Last := 22326;
      result := true;
    end;
    1121: begin
      First := 22327;
      Last := 22343;
      result := true;
    end;
    1122: begin
      First := 22344;
      Last := 22349;
      result := true;
    end;
    1123: begin
      First := 22350;
      Last := 22383;
      result := true;
    end;
    1124: begin
      First := 22384;
      Last := 22390;
      result := true;
    end;
    1125: begin
      First := 22391;
      Last := 22399;
      result := true;
    end;
    1126: begin
      First := 22400;
      Last := 22405;
      result := true;
    end;
    1127: begin
      First := 22406;
      Last := 22411;
      result := true;
    end;
    1128: begin
      First := 22412;
      Last := 22417;
      result := true;
    end;
    1129: begin
      First := 22418;
      Last := 22426;
      result := true;
    end;
    1130: begin
      First := 22427;
      Last := 22433;
      result := true;
    end;
    1131: begin
      First := 22434;
      Last := 22439;
      result := true;
    end;
    1132: begin
      First := 22440;
      Last := 22485;
      result := true;
    end;
    1133: begin
      First := 22486;
      Last := 22491;
      result := true;
    end;
    1134: begin
      First := 22492;
      Last := 22537;
      result := true;
    end;
    1135: begin
      First := 22538;
      Last := 22543;
      result := true;
    end;
    1136: begin
      First := 22544;
      Last := 22549;
      result := true;
    end;
    1137: begin
      First := 22550;
      Last := 22556;
      result := true;
    end;
    1138: begin
      First := 22557;
      Last := 22563;
      result := true;
    end;
    1139: begin
      First := 22564;
      Last := 22569;
      result := true;
    end;
    1140: begin
      First := 22570;
      Last := 22576;
      result := true;
    end;
    1141: begin
      First := 22577;
      Last := 22582;
      result := true;
    end;
    1142: begin
      First := 22583;
      Last := 22589;
      result := true;
    end;
    1143: begin
      First := 22590;
      Last := 22595;
      result := true;
    end;
    1144: begin
      First := 22596;
      Last := 22601;
      result := true;
    end;
    1145: begin
      First := 22602;
      Last := 22612;
      result := true;
    end;
    1146: begin
      First := 22613;
      Last := 22618;
      result := true;
    end;
    1147: begin
      First := 22619;
      Last := 22626;
      result := true;
    end;
    1148: begin
      First := 22627;
      Last := 22635;
      result := true;
    end;
    1149: begin
      First := 22636;
      Last := 22645;
      result := true;
    end;
    1150: begin
      First := 22646;
      Last := 22654;
      result := true;
    end;
    1151: begin
      First := 22655;
      Last := 22664;
      result := true;
    end;
    1152: begin
      First := 22665;
      Last := 22675;
      result := true;
    end;
    1153: begin
      First := 22676;
      Last := 22685;
      result := true;
    end;
    1154: begin
      First := 22686;
      Last := 22692;
      result := true;
    end;
    1155: begin
      First := 22693;
      Last := 22701;
      result := true;
    end;
    1156: begin
      First := 22702;
      Last := 22707;
      result := true;
    end;
    1157: begin
      First := 22708;
      Last := 22713;
      result := true;
    end;
    1158: begin
      First := 22714;
      Last := 22730;
      result := true;
    end;
    1159: begin
      First := 22731;
      Last := 22737;
      result := true;
    end;
    1160: begin
      First := 22738;
      Last := 22770;
      result := true;
    end;
    1161: begin
      First := 22771;
      Last := 22779;
      result := true;
    end;
    1162: begin
      First := 22780;
      Last := 22785;
      result := true;
    end;
    1163: begin
      First := 22786;
      Last := 22797;
      result := true;
    end;
    1164: begin
      First := 22798;
      Last := 22803;
      result := true;
    end;
    1165: begin
      First := 22804;
      Last := 22809;
      result := true;
    end;
    1166: begin
      First := 22810;
      Last := 22948;
      result := true;
    end;
    1167: begin
      First := 22949;
      Last := 22966;
      result := true;
    end;
    1168: begin
      First := 22967;
      Last := 22972;
      result := true;
    end;
    1169: begin
      First := 22973;
      Last := 22989;
      result := true;
    end;
    1170: begin
      First := 22990;
      Last := 22995;
      result := true;
    end;
    1171: begin
      First := 22996;
      Last := 23017;
      result := true;
    end;
    1172: begin
      First := 23018;
      Last := 23024;
      result := true;
    end;
    1173: begin
      First := 23025;
      Last := 23030;
      result := true;
    end;
    1174: begin
      First := 23031;
      Last := 23052;
      result := true;
    end;
    1175: begin
      First := 23053;
      Last := 23064;
      result := true;
    end;
    1176: begin
      First := 23065;
      Last := 23071;
      result := true;
    end;
    1177: begin
      First := 23072;
      Last := 23082;
      result := true;
    end;
    1178: begin
      First := 23083;
      Last := 23089;
      result := true;
    end;
    1179: begin
      First := 23090;
      Last := 23111;
      result := true;
    end;
    1180: begin
      First := 23112;
      Last := 23122;
      result := true;
    end;
    1181: begin
      First := 23123;
      Last := 23133;
      result := true;
    end;
    1182: begin
      First := 23134;
      Last := 23140;
      result := true;
    end;
    1183: begin
      First := 23141;
      Last := 23147;
      result := true;
    end;
    1184: begin
      First := 23148;
      Last := 23206;
      result := true;
    end;
    1185: begin
      First := 23207;
      Last := 23212;
      result := true;
    end;
    1186: begin
      First := 23213;
      Last := 23218;
      result := true;
    end;
    1187: begin
      First := 23219;
      Last := 23246;
      result := true;
    end;
    1188: begin
      First := 23247;
      Last := 23252;
      result := true;
    end;
    1189: begin
      First := 23253;
      Last := 23259;
      result := true;
    end;
    1190: begin
      First := 23260;
      Last := 23265;
      result := true;
    end;
    1191: begin
      First := 23266;
      Last := 23271;
      result := true;
    end;
    1192: begin
      First := 23272;
      Last := 23280;
      result := true;
    end;
    1193: begin
      First := 23281;
      Last := 23286;
      result := true;
    end;
    1194: begin
      First := 23287;
      Last := 23292;
      result := true;
    end;
    1195: begin
      First := 23293;
      Last := 23411;
      result := true;
    end;
    1196: begin
      First := 23412;
      Last := 23466;
      result := true;
    end;
    1197: begin
      First := 23467;
      Last := 23472;
      result := true;
    end;
    1198: begin
      First := 23473;
      Last := 23591;
      result := true;
    end;
    1199: begin
      First := 23592;
      Last := 23600;
      result := true;
    end;
    1200: begin
      First := 23601;
      Last := 23614;
      result := true;
    end;
    1201: begin
      First := 23615;
      Last := 23620;
      result := true;
    end;
    1202: begin
      First := 23621;
      Last := 23629;
      result := true;
    end;
    1203: begin
      First := 23630;
      Last := 23635;
      result := true;
    end;
    1204: begin
      First := 23636;
      Last := 23641;
      result := true;
    end;
    1205: begin
      First := 23642;
      Last := 23648;
      result := true;
    end;
    1206: begin
      First := 23649;
      Last := 23654;
      result := true;
    end;
    1207: begin
      First := 23655;
      Last := 23660;
      result := true;
    end;
    1208: begin
      First := 23661;
      Last := 23666;
      result := true;
    end;
    1209: begin
      First := 23667;
      Last := 23672;
      result := true;
    end;
    1210: begin
      First := 23673;
      Last := 23678;
      result := true;
    end;
    1211: begin
      First := 23679;
      Last := 23684;
      result := true;
    end;
    1212: begin
      First := 23685;
      Last := 23690;
      result := true;
    end;
    1213: begin
      First := 23691;
      Last := 23697;
      result := true;
    end;
    1214: begin
      First := 23698;
      Last := 23816;
      result := true;
    end;
    1215: begin
      First := 23817;
      Last := 23822;
      result := true;
    end;
    1216: begin
      First := 23823;
      Last := 23828;
      result := true;
    end;
    1217: begin
      First := 23829;
      Last := 23834;
      result := true;
    end;
    1218: begin
      First := 23835;
      Last := 23842;
      result := true;
    end;
    1219: begin
      First := 23843;
      Last := 23848;
      result := true;
    end;
    1220: begin
      First := 23849;
      Last := 23854;
      result := true;
    end;
    1221: begin
      First := 23855;
      Last := 23860;
      result := true;
    end;
    1222: begin
      First := 23861;
      Last := 23866;
      result := true;
    end;
    1223: begin
      First := 23867;
      Last := 23872;
      result := true;
    end;
    1224: begin
      First := 23873;
      Last := 23878;
      result := true;
    end;
    1225: begin
      First := 23879;
      Last := 23884;
      result := true;
    end;
    1226: begin
      First := 23885;
      Last := 23890;
      result := true;
    end;
    1227: begin
      First := 23891;
      Last := 23896;
      result := true;
    end;
    1228: begin
      First := 23897;
      Last := 23902;
      result := true;
    end;
    1229: begin
      First := 23903;
      Last := 23908;
      result := true;
    end;
    1230: begin
      First := 23909;
      Last := 23914;
      result := true;
    end;
    1231: begin
      First := 23915;
      Last := 23920;
      result := true;
    end;
    1232: begin
      First := 23921;
      Last := 23926;
      result := true;
    end;
    1233: begin
      First := 23927;
      Last := 23932;
      result := true;
    end;
    1234: begin
      First := 23933;
      Last := 23939;
      result := true;
    end;
    1235: begin
      First := 23940;
      Last := 23954;
      result := true;
    end;
    1236: begin
      First := 23955;
      Last := 23962;
      result := true;
    end;
    1237: begin
      First := 23963;
      Last := 23982;
      result := true;
    end;
    1238: begin
      First := 23983;
      Last := 23997;
      result := true;
    end;
    1239: begin
      First := 23998;
      Last := 24058;
      result := true;
    end;
    1240: begin
      First := 24059;
      Last := 24077;
      result := true;
    end;
    1241: begin
      First := 24078;
      Last := 24083;
      result := true;
    end;
    1242: begin
      First := 24084;
      Last := 24096;
      result := true;
    end;
    1243: begin
      First := 24097;
      Last := 24102;
      result := true;
    end;
    1244: begin
      First := 24103;
      Last := 24111;
      result := true;
    end;
    1245: begin
      First := 24112;
      Last := 24156;
      result := true;
    end;
    1246: begin
      First := 24157;
      Last := 24163;
      result := true;
    end;
    1247: begin
      First := 24164;
      Last := 24172;
      result := true;
    end;
    1248: begin
      First := 24173;
      Last := 24190;
      result := true;
    end;
    1249: begin
      First := 24191;
      Last := 24196;
      result := true;
    end;
    1250: begin
      First := 24197;
      Last := 24202;
      result := true;
    end;
    1251: begin
      First := 24203;
      Last := 24209;
      result := true;
    end;
    1252: begin
      First := 24210;
      Last := 24216;
      result := true;
    end;
    1253: begin
      First := 24217;
      Last := 24222;
      result := true;
    end;
    1254: begin
      First := 24223;
      Last := 24248;
      result := true;
    end;
    1255: begin
      First := 24249;
      Last := 24273;
      result := true;
    end;
    1256: begin
      First := 24274;
      Last := 24279;
      result := true;
    end;
    1257: begin
      First := 24280;
      Last := 24286;
      result := true;
    end;
    1258: begin
      First := 24287;
      Last := 24293;
      result := true;
    end;
    1259: begin
      First := 24294;
      Last := 24309;
      result := true;
    end;
    1260: begin
      First := 24310;
      Last := 24316;
      result := true;
    end;
    1261: begin
      First := 24317;
      Last := 24351;
      result := true;
    end;
    1262: begin
      First := 24352;
      Last := 24357;
      result := true;
    end;
    1263: begin
      First := 24358;
      Last := 24363;
      result := true;
    end;
    1264: begin
      First := 24364;
      Last := 24369;
      result := true;
    end;
    1265: begin
      First := 24370;
      Last := 24375;
      result := true;
    end;
    1266: begin
      First := 24376;
      Last := 24385;
      result := true;
    end;
    1267: begin
      First := 24386;
      Last := 24391;
      result := true;
    end;
    1268: begin
      First := 24392;
      Last := 24397;
      result := true;
    end;
    1269: begin
      First := 24398;
      Last := 24403;
      result := true;
    end;
    1270: begin
      First := 24404;
      Last := 24410;
      result := true;
    end;
    1271: begin
      First := 24411;
      Last := 24416;
      result := true;
    end;
    1272: begin
      First := 24417;
      Last := 24422;
      result := true;
    end;
    1273: begin
      First := 24423;
      Last := 24444;
      result := true;
    end;
    1274: begin
      First := 24445;
      Last := 24451;
      result := true;
    end;
    1275: begin
      First := 24452;
      Last := 24458;
      result := true;
    end;
    1276: begin
      First := 24459;
      Last := 24465;
      result := true;
    end;
    1277: begin
      First := 24466;
      Last := 24474;
      result := true;
    end;
    1278: begin
      First := 24475;
      Last := 24484;
      result := true;
    end;
    1279: begin
      First := 24485;
      Last := 24495;
      result := true;
    end;
    1280: begin
      First := 24496;
      Last := 24506;
      result := true;
    end;
    1281: begin
      First := 24507;
      Last := 24512;
      result := true;
    end;
    1282: begin
      First := 24513;
      Last := 24525;
      result := true;
    end;
    1283: begin
      First := 24526;
      Last := 24532;
      result := true;
    end;
    1284: begin
      First := 24533;
      Last := 24538;
      result := true;
    end;
    1285: begin
      First := 24539;
      Last := 24544;
      result := true;
    end;
    1286: begin
      First := 24545;
      Last := 24553;
      result := true;
    end;
    1287: begin
      First := 24554;
      Last := 24559;
      result := true;
    end;
    1288: begin
      First := 24560;
      Last := 24566;
      result := true;
    end;
    1289: begin
      First := 24567;
      Last := 24572;
      result := true;
    end;
    1290: begin
      First := 24573;
      Last := 24578;
      result := true;
    end;
    1291: begin
      First := 24579;
      Last := 24616;
      result := true;
    end;
    1292: begin
      First := 24617;
      Last := 24622;
      result := true;
    end;
    1293: begin
      First := 24623;
      Last := 24661;
      result := true;
    end;
    1294: begin
      First := 24662;
      Last := 24704;
      result := true;
    end;
    1295: begin
      First := 24705;
      Last := 24711;
      result := true;
    end;
    1296: begin
      First := 24712;
      Last := 24720;
      result := true;
    end;
    1297: begin
      First := 24721;
      Last := 24726;
      result := true;
    end;
    1298: begin
      First := 24727;
      Last := 24748;
      result := true;
    end;
    1299: begin
      First := 24749;
      Last := 24760;
      result := true;
    end;
    1300: begin
      First := 24761;
      Last := 24766;
      result := true;
    end;
    1301: begin
      First := 24767;
      Last := 24772;
      result := true;
    end;
    1302: begin
      First := 24773;
      Last := 24778;
      result := true;
    end;
    1303: begin
      First := 24779;
      Last := 24917;
      result := true;
    end;
    1304: begin
      First := 24918;
      Last := 24924;
      result := true;
    end;
    1305: begin
      First := 24925;
      Last := 24946;
      result := true;
    end;
    1306: begin
      First := 24947;
      Last := 24953;
      result := true;
    end;
    1307: begin
      First := 24954;
      Last := 24975;
      result := true;
    end;
    1308: begin
      First := 24976;
      Last := 24981;
      result := true;
    end;
    1309: begin
      First := 24982;
      Last := 24988;
      result := true;
    end;
    1310: begin
      First := 24989;
      Last := 24995;
      result := true;
    end;
    1311: begin
      First := 24996;
      Last := 25017;
      result := true;
    end;
    1312: begin
      First := 25018;
      Last := 25039;
      result := true;
    end;
    1313: begin
      First := 25040;
      Last := 25046;
      result := true;
    end;
    1314: begin
      First := 25047;
      Last := 25052;
      result := true;
    end;
    1315: begin
      First := 25053;
      Last := 25058;
      result := true;
    end;
    1316: begin
      First := 25059;
      Last := 25115;
      result := true;
    end;
    1317: begin
      First := 25116;
      Last := 25121;
      result := true;
    end;
    1318: begin
      First := 25122;
      Last := 25127;
      result := true;
    end;
    1319: begin
      First := 25128;
      Last := 25133;
      result := true;
    end;
    1320: begin
      First := 25134;
      Last := 25251;
      result := true;
    end;
    1321: begin
      First := 25252;
      Last := 25257;
      result := true;
    end;
    1322: begin
      First := 25258;
      Last := 25376;
      result := true;
    end;
    1323: begin
      First := 25377;
      Last := 25382;
      result := true;
    end;
    1324: begin
      First := 25383;
      Last := 25388;
      result := true;
    end;
    1325: begin
      First := 25389;
      Last := 25401;
      result := true;
    end;
    1326: begin
      First := 25402;
      Last := 25431;
      result := true;
    end;
    1327: begin
      First := 25432;
      Last := 25437;
      result := true;
    end;
    1328: begin
      First := 25438;
      Last := 25443;
      result := true;
    end;
    1329: begin
      First := 25444;
      Last := 25462;
      result := true;
    end;
    1330: begin
      First := 25463;
      Last := 25469;
      result := true;
    end;
    1331: begin
      First := 25470;
      Last := 25484;
      result := true;
    end;
    1332: begin
      First := 25485;
      Last := 25527;
      result := true;
    end;
    1333: begin
      First := 25528;
      Last := 25533;
      result := true;
    end;
    1334: begin
      First := 25534;
      Last := 25539;
      result := true;
    end;
    1335: begin
      First := 25540;
      Last := 25545;
      result := true;
    end;
    1336: begin
      First := 25546;
      Last := 25551;
      result := true;
    end;
    1337: begin
      First := 25552;
      Last := 25557;
      result := true;
    end;
    1338: begin
      First := 25558;
      Last := 25600;
      result := true;
    end;
    1339: begin
      First := 25601;
      Last := 25606;
      result := true;
    end;
    1340: begin
      First := 25607;
      Last := 25730;
      result := true;
    end;
    1341: begin
      First := 25731;
      Last := 25738;
      result := true;
    end;
    1342: begin
      First := 25739;
      Last := 25744;
      result := true;
    end;
    1343: begin
      First := 25745;
      Last := 25750;
      result := true;
    end;
    1344: begin
      First := 25751;
      Last := 25766;
      result := true;
    end;
    1345: begin
      First := 25767;
      Last := 25772;
      result := true;
    end;
    1346: begin
      First := 25773;
      Last := 25808;
      result := true;
    end;
    1347: begin
      First := 25809;
      Last := 25814;
      result := true;
    end;
    1348: begin
      First := 25815;
      Last := 25821;
      result := true;
    end;
    1349: begin
      First := 25822;
      Last := 25830;
      result := true;
    end;
    1350: begin
      First := 25831;
      Last := 25836;
      result := true;
    end;
    1351: begin
      First := 25837;
      Last := 25842;
      result := true;
    end;
    1352: begin
      First := 25843;
      Last := 25848;
      result := true;
    end;
    1353: begin
      First := 25849;
      Last := 25854;
      result := true;
    end;
    1354: begin
      First := 25855;
      Last := 25860;
      result := true;
    end;
    1355: begin
      First := 25861;
      Last := 25866;
      result := true;
    end;
    1356: begin
      First := 25867;
      Last := 25872;
      result := true;
    end;
    1357: begin
      First := 25873;
      Last := 25879;
      result := true;
    end;
    1358: begin
      First := 25880;
      Last := 25896;
      result := true;
    end;
    1359: begin
      First := 25897;
      Last := 25902;
      result := true;
    end;
    1360: begin
      First := 25903;
      Last := 25909;
      result := true;
    end;
    1361: begin
      First := 25910;
      Last := 25916;
      result := true;
    end;
    1362: begin
      First := 25917;
      Last := 25925;
      result := true;
    end;
    1363: begin
      First := 25926;
      Last := 25931;
      result := true;
    end;
    1364: begin
      First := 25932;
      Last := 25937;
      result := true;
    end;
    1365: begin
      First := 25938;
      Last := 25943;
      result := true;
    end;
    1366: begin
      First := 25944;
      Last := 25949;
      result := true;
    end;
    1367: begin
      First := 25950;
      Last := 25955;
      result := true;
    end;
    1368: begin
      First := 25956;
      Last := 25961;
      result := true;
    end;
    1369: begin
      First := 25962;
      Last := 25968;
      result := true;
    end;
    1370: begin
      First := 25969;
      Last := 25974;
      result := true;
    end;
    1371: begin
      First := 25975;
      Last := 25981;
      result := true;
    end;
    1372: begin
      First := 25982;
      Last := 25987;
      result := true;
    end;
    1373: begin
      First := 25988;
      Last := 25994;
      result := true;
    end;
    1374: begin
      First := 25995;
      Last := 26001;
      result := true;
    end;
    1375: begin
      First := 26002;
      Last := 26008;
      result := true;
    end;
    1376: begin
      First := 26009;
      Last := 26018;
      result := true;
    end;
    1377: begin
      First := 26019;
      Last := 26024;
      result := true;
    end;
    1378: begin
      First := 26025;
      Last := 26033;
      result := true;
    end;
    1379: begin
      First := 26034;
      Last := 26039;
      result := true;
    end;
    1380: begin
      First := 26040;
      Last := 26056;
      result := true;
    end;
    1381: begin
      First := 26057;
      Last := 26062;
      result := true;
    end;
    1382: begin
      First := 26063;
      Last := 26068;
      result := true;
    end;
    1383: begin
      First := 26069;
      Last := 26080;
      result := true;
    end;
    1384: begin
      First := 26081;
      Last := 26086;
      result := true;
    end;
    1385: begin
      First := 26087;
      Last := 26135;
      result := true;
    end;
    1386: begin
      First := 26136;
      Last := 26152;
      result := true;
    end;
    1387: begin
      First := 26153;
      Last := 26164;
      result := true;
    end;
    1388: begin
      First := 26165;
      Last := 26186;
      result := true;
    end;
    1389: begin
      First := 26187;
      Last := 26192;
      result := true;
    end;
    1390: begin
      First := 26193;
      Last := 26214;
      result := true;
    end;
    1391: begin
      First := 26215;
      Last := 26236;
      result := true;
    end;
    1392: begin
      First := 26237;
      Last := 26242;
      result := true;
    end;
    1393: begin
      First := 26243;
      Last := 26254;
      result := true;
    end;
    1394: begin
      First := 26255;
      Last := 26260;
      result := true;
    end;
    1395: begin
      First := 26261;
      Last := 26274;
      result := true;
    end;
    1396: begin
      First := 26275;
      Last := 26280;
      result := true;
    end;
    1397: begin
      First := 26281;
      Last := 26286;
      result := true;
    end;
    1398: begin
      First := 26287;
      Last := 26292;
      result := true;
    end;
    1399: begin
      First := 26293;
      Last := 26308;
      result := true;
    end;
    1400: begin
      First := 26309;
      Last := 26314;
      result := true;
    end;
    1401: begin
      First := 26315;
      Last := 26327;
      result := true;
    end;
    1402: begin
      First := 26328;
      Last := 26333;
      result := true;
    end;
    1403: begin
      First := 26334;
      Last := 26340;
      result := true;
    end;
    1404: begin
      First := 26341;
      Last := 26346;
      result := true;
    end;
    1405: begin
      First := 26347;
      Last := 26352;
      result := true;
    end;
    1406: begin
      First := 26353;
      Last := 26359;
      result := true;
    end;
    1407: begin
      First := 26360;
      Last := 26378;
      result := true;
    end;
    1408: begin
      First := 26379;
      Last := 26384;
      result := true;
    end;
    1409: begin
      First := 26385;
      Last := 26390;
      result := true;
    end;
    1410: begin
      First := 26391;
      Last := 26396;
      result := true;
    end;
    1411: begin
      First := 26397;
      Last := 26435;
      result := true;
    end;
    1412: begin
      First := 26436;
      Last := 26442;
      result := true;
    end;
    1413: begin
      First := 26443;
      Last := 26461;
      result := true;
    end;
    1414: begin
      First := 26462;
      Last := 26467;
      result := true;
    end;
    1415: begin
      First := 26468;
      Last := 26476;
      result := true;
    end;
    1416: begin
      First := 26477;
      Last := 26482;
      result := true;
    end;
    1417: begin
      First := 26483;
      Last := 26489;
      result := true;
    end;
    1418: begin
      First := 26490;
      Last := 26495;
      result := true;
    end;
    1419: begin
      First := 26496;
      Last := 26511;
      result := true;
    end;
    1420: begin
      First := 26512;
      Last := 26517;
      result := true;
    end;
    1421: begin
      First := 26518;
      Last := 26524;
      result := true;
    end;
    1422: begin
      First := 26525;
      Last := 26530;
      result := true;
    end;
    1423: begin
      First := 26531;
      Last := 26536;
      result := true;
    end;
    1424: begin
      First := 26537;
      Last := 26542;
      result := true;
    end;
    1425: begin
      First := 26543;
      Last := 26548;
      result := true;
    end;
    1426: begin
      First := 26549;
      Last := 26554;
      result := true;
    end;
    1427: begin
      First := 26555;
      Last := 26563;
      result := true;
    end;
    1428: begin
      First := 26564;
      Last := 26569;
      result := true;
    end;
    1429: begin
      First := 26570;
      Last := 26575;
      result := true;
    end;
    1430: begin
      First := 26576;
      Last := 26582;
      result := true;
    end;
    1431: begin
      First := 26583;
      Last := 26591;
      result := true;
    end;
    1432: begin
      First := 26592;
      Last := 26597;
      result := true;
    end;
    1433: begin
      First := 26598;
      Last := 26603;
      result := true;
    end;
    1434: begin
      First := 26604;
      Last := 26609;
      result := true;
    end;
    1435: begin
      First := 26610;
      Last := 26615;
      result := true;
    end;
    1436: begin
      First := 26616;
      Last := 26621;
      result := true;
    end;
    1437: begin
      First := 26622;
      Last := 26632;
      result := true;
    end;
    1438: begin
      First := 26633;
      Last := 26773;
      result := true;
    end;
    1439: begin
      First := 26774;
      Last := 26785;
      result := true;
    end;
    1440: begin
      First := 26786;
      Last := 26797;
      result := true;
    end;
    1441: begin
      First := 26798;
      Last := 26812;
      result := true;
    end;
    1442: begin
      First := 26813;
      Last := 26818;
      result := true;
    end;
    1443: begin
      First := 26819;
      Last := 26824;
      result := true;
    end;
    1444: begin
      First := 26825;
      Last := 26867;
      result := true;
    end;
    1445: begin
      First := 26868;
      Last := 26906;
      result := true;
    end;
    1446: begin
      First := 26907;
      Last := 26912;
      result := true;
    end;
    1447: begin
      First := 26913;
      Last := 26919;
      result := true;
    end;
    1448: begin
      First := 26920;
      Last := 26925;
      result := true;
    end;
    1449: begin
      First := 26926;
      Last := 26942;
      result := true;
    end;
    1450: begin
      First := 26943;
      Last := 26948;
      result := true;
    end;
    1451: begin
      First := 26949;
      Last := 26954;
      result := true;
    end;
    1452: begin
      First := 26955;
      Last := 26963;
      result := true;
    end;
    1453: begin
      First := 26964;
      Last := 26970;
      result := true;
    end;
    1454: begin
      First := 26971;
      Last := 26976;
      result := true;
    end;
    1455: begin
      First := 26977;
      Last := 26985;
      result := true;
    end;
    1456: begin
      First := 26986;
      Last := 26991;
      result := true;
    end;
    1457: begin
      First := 26992;
      Last := 27003;
      result := true;
    end;
    1458: begin
      First := 27004;
      Last := 27009;
      result := true;
    end;
    1459: begin
      First := 27010;
      Last := 27150;
      result := true;
    end;
    1460: begin
      First := 27151;
      Last := 27156;
      result := true;
    end;
    1461: begin
      First := 27157;
      Last := 27173;
      result := true;
    end;
    1462: begin
      First := 27174;
      Last := 27214;
      result := true;
    end;
    1463: begin
      First := 27215;
      Last := 27224;
      result := true;
    end;
    1464: begin
      First := 27225;
      Last := 27230;
      result := true;
    end;
    1465: begin
      First := 27231;
      Last := 27237;
      result := true;
    end;
    1466: begin
      First := 27238;
      Last := 27243;
      result := true;
    end;
    1467: begin
      First := 27244;
      Last := 27250;
      result := true;
    end;
    1468: begin
      First := 27251;
      Last := 27261;
      result := true;
    end;
    1469: begin
      First := 27262;
      Last := 27276;
      result := true;
    end;
    1470: begin
      First := 27277;
      Last := 27287;
      result := true;
    end;
    1471: begin
      First := 27288;
      Last := 27293;
      result := true;
    end;
    1472: begin
      First := 27294;
      Last := 27304;
      result := true;
    end;
    1473: begin
      First := 27305;
      Last := 27311;
      result := true;
    end;
    1474: begin
      First := 27312;
      Last := 27318;
      result := true;
    end;
    1475: begin
      First := 27319;
      Last := 27357;
      result := true;
    end;
    1476: begin
      First := 27358;
      Last := 27396;
      result := true;
    end;
    1477: begin
      First := 27397;
      Last := 27402;
      result := true;
    end;
    1478: begin
      First := 27403;
      Last := 27408;
      result := true;
    end;
    1479: begin
      First := 27409;
      Last := 27417;
      result := true;
    end;
    1480: begin
      First := 27418;
      Last := 27423;
      result := true;
    end;
    1481: begin
      First := 27424;
      Last := 27429;
      result := true;
    end;
    1482: begin
      First := 27430;
      Last := 27435;
      result := true;
    end;
    1483: begin
      First := 27436;
      Last := 27442;
      result := true;
    end;
    1484: begin
      First := 27443;
      Last := 27448;
      result := true;
    end;
    1485: begin
      First := 27449;
      Last := 27454;
      result := true;
    end;
    1486: begin
      First := 27455;
      Last := 27470;
      result := true;
    end;
    1487: begin
      First := 27471;
      Last := 27476;
      result := true;
    end;
    1488: begin
      First := 27477;
      Last := 27482;
      result := true;
    end;
    1489: begin
      First := 27483;
      Last := 27488;
      result := true;
    end;
    1490: begin
      First := 27489;
      Last := 27503;
      result := true;
    end;
    1491: begin
      First := 27504;
      Last := 27509;
      result := true;
    end;
    1492: begin
      First := 27510;
      Last := 27515;
      result := true;
    end;
    1493: begin
      First := 27516;
      Last := 27521;
      result := true;
    end;
    1494: begin
      First := 27522;
      Last := 27528;
      result := true;
    end;
    1495: begin
      First := 27529;
      Last := 27534;
      result := true;
    end;
    1496: begin
      First := 27535;
      Last := 27540;
      result := true;
    end;
    1497: begin
      First := 27541;
      Last := 27547;
      result := true;
    end;
    1498: begin
      First := 27548;
      Last := 27558;
      result := true;
    end;
    1499: begin
      First := 27559;
      Last := 27569;
      result := true;
    end;
    1500: begin
      First := 27570;
      Last := 27575;
      result := true;
    end;
    1501: begin
      First := 27576;
      Last := 27584;
      result := true;
    end;
    1502: begin
      First := 27585;
      Last := 27590;
      result := true;
    end;
    1503: begin
      First := 27591;
      Last := 27596;
      result := true;
    end;
    1504: begin
      First := 27597;
      Last := 27602;
      result := true;
    end;
    1505: begin
      First := 27603;
      Last := 27609;
      result := true;
    end;
    1506: begin
      First := 27610;
      Last := 27618;
      result := true;
    end;
    1507: begin
      First := 27619;
      Last := 27624;
      result := true;
    end;
    1508: begin
      First := 27625;
      Last := 27630;
      result := true;
    end;
    1509: begin
      First := 27631;
      Last := 27636;
      result := true;
    end;
    1510: begin
      First := 27637;
      Last := 27643;
      result := true;
    end;
  else
    First := -1; Last := -1;
    result := false;
  end;
end;

function GetStateDebug(state: integer):TStringList;
var
  First, Last, i: integer;
begin
  if LookupOffsets(state, First, Last) then
  begin
    result := TStringList.Create;
    Assert(Last >= First);
    for i := First to Last do
      result.Add(ListInfo[i]);
  end
  else
    result := nil;
end;

end.

