use strict;
use warnings;

use Test::More;
if($ENV{ENTITYMODEL_PG_HOST}) {
	plan tests => 14;
} else {
	plan skip_all => 'No PostgreSQL connection details found, please set ENTITYMODEL_PG_* to test';
}

use EntityModel::Log;
# EntityModel::Log->instance->min_level(0);

use EntityModel;
ok(my $model = EntityModel->new->load_from(
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
   { "name" => "extra", "type" => "varchar" }
  ] }
  ] }
), 'load model');
isa_ok($model, 'EntityModel::Model');
ok($model->add_storage(PostgreSQL => {
	schema	=> 'emtest',
	user	=> $ENV{ENTITYMODEL_PG_USER},
	pass	=> $ENV{ENTITYMODEL_PG_PASS},
	host	=> $ENV{ENTITYMODEL_PG_HOST},
}), 'add PostgreSQL storage');

is($model->storage->count, 1, 'have single storage entry');
my ($storage) = $model->storage->list;
isa_ok($storage, 'EntityModel::Storage');
isa_ok($storage, 'EntityModel::Storage::PostgreSQL');

# Create our classes so we can get to the data
ok($model->add_support(Perl => { }), 'set up Perl class access');
ok(my $entity = Entity::Thing->create({name => "test"}), 'create new instance');
ok(!$entity->idthing, 'no ID before commit');
ok($entity->commit, 'can commit');
ok($entity->id, 'has ID after commit');
ok(my $e = Entity::Thing->new($entity->id), 'instantiate');
is($e->id, $entity->id, 'id matches');
is($e->name, $entity->name, 'name matches');
