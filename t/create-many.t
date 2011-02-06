use strict;
use warnings;

use Test::More;
use Test::Deep;
if($ENV{ENTITYMODEL_PG_HOST}) {
	plan tests => 6;
} else {
	plan skip_all => 'No PostgreSQL connection details found, please set ENTITYMODEL_PG_* to test';
}

use EntityModel::Log;
# EntityModel::Log->instance->min_level(0);

use EntityModel;
my $model;
BEGIN {
	$model = EntityModel->new->load_from(
		Perl	=> {
	 "name" => "mymodel",
	 "entity" => [ {
	  "name" => "thing",
	  "primary" => "idthing",
	  "field" => [
	   { "name" => "idthing", "type" => "bigserial" },
	   { "name" => "name", "type" => "varchar" }
	  ] }, {
	  "name" => "other",
	  "primary" => "idother",
	  "field" => [
	   { "name" => "idother", "type" => "bigserial" },
	   { "name" => "extra", "type" => "varchar" },
	   { "name" => "idthing", "type" => "bigint", refer => [ { table => "thing", field => "idthing", delete => "cascade", update => "cascade" } ] },
	  ] }
	  ] }
	);

	$model->add_storage(PostgreSQL => {
		schema	=> 'emtest',
		user	=> $ENV{ENTITYMODEL_PG_USER},
		pass	=> $ENV{ENTITYMODEL_PG_PASS},
		host	=> $ENV{ENTITYMODEL_PG_HOST},
	});
	$model->add_support(Perl => { });
}

isa_ok($model, 'EntityModel::Model');
is($model->storage->count, 1, 'have single storage entry');
my ($storage) = $model->storage->list;
isa_ok($storage, 'EntityModel::Storage');
isa_ok($storage, 'EntityModel::Storage::PostgreSQL');

my @entities = map { Entity::Thing->create({name => "test $_"})->commit } 0..99;
is(@entities, 100, 'create 100 entities');

my %uniq;
$uniq{$_}++ for map { $_->id } @entities;
fail("$_ is not unique (" . $uniq{$_} . " copies)") for grep { $uniq{$_} > 1 } keys %uniq;

my @instance = map { Entity::Thing->new($_)->id } map { $_->id } @entities;
cmp_deeply([ @instance ], [ map { $_->id } @entities ], 'IDs match after reinstantiation');

$storage->remove_schema;
$storage->dbh->commit;

