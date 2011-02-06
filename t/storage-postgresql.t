use strict;
use warnings;

use Test::More;
if($ENV{ENTITYMODEL_PG_HOST}) {
	plan tests => 22;
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
   { "name" => "extra", "type" => "varchar" },
   { "name" => "idthing", "type" => "bigint", refer => [ { table => "thing", field => "idthing", delete => "cascade", update => "cascade" } ] },
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

#note $entity->id;
#ok(!$entity->id, 'no ID before commit');
ok($entity->commit, 'can commit');
ok($entity->id, 'has ID after commit');
note "ID was " . $entity->id;
ok(my $e = Entity::Thing->new($entity->id), 'instantiate');
is($e->id, $entity->id, 'id matches');
is($e->name, $entity->name, 'name matches');

ok(my $other = Entity::Other->create({extra => "something here", thing => $entity }), 'create new instance');
ok($other->commit, 'commit entry');
is($other->extra, 'something here', '->extra matches');
is($other->thing->id, $entity->id, 'id matches');
note "ID was " . $other->id;

ok($other->extra("changed"), 'change ->extra');
is($other->extra, "changed", 'value is updated');
ok($other->commit, 'can commit');
is($other->extra, "changed", 'value is the same after commit');

$storage->remove_schema;
$storage->dbh->commit;

