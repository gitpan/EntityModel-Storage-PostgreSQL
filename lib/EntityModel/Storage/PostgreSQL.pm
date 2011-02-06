package EntityModel::Storage::PostgreSQL;
use EntityModel::Class {
	_isa		=> [qw{EntityModel::Storage}],
	db		=> { type => 'EntityModel::DB' },
	schema		=> { type => 'string' },
	field_cache	=> { type => 'hash' },
	primary_cache	=> { type => 'array' },
};

our $VERSION = '0.003';

=head1 NAME

EntityModel::Storage::PostgreSQL - backend storage interface for L<EntityModel>

=head1 SYNOPSIS

See L<EntityModel>.

=head1 DESCRIPTION

See L<EntityModel>.

=cut

use Scalar::Util ();

=head1 METHODS

=cut

=head2 setup

Open the initial database connection and set schema if provided.

=cut

sub setup {
	my $self = shift;
	my %args = %{+shift};

# If we have a DB object already, just use that.
	$self->db(delete $args{db}) if exists $args{db};
	$self->schema(delete $args{schema}) if exists $args{schema};

# Otherwise we'll need to pick one up from the parameters
	$self->db(EntityModel::DB->new(
		user		=> delete $args{user},
		password	=> delete $args{password},
		host		=> delete $args{host},
		port		=> delete $args{port},
		dbname		=> delete $args{dbname}
	)) unless $self->db;

# Without a database, we can't do much, bail out here
	die "no db" unless $self->db;
	return $self;
}

=head2 apply_model

Applies the requested model to the database.

=cut

sub apply_model {
	my $self = shift;
	my $model = shift;
	logDebug("Apply model");
	Scalar::Util::weaken $self;
	Scalar::Util::weaken $model;
	$model->transaction(sub {
		$self->apply_model_and_schema($model);
	});
}

=head2 apply_model_and_schema

Given a model, apply it to the database, optionally creating the requested schema.

=cut

sub apply_model_and_schema {
	my $self = shift;
	my $model = shift;
	$self->create_schema if $self->schema && !$self->schema_exists;

	my @pending = $model->entity->list;
	my @existing;

	ITEM:
	while(@pending) {
		my $entity = shift(@pending);

		my @deps = $entity->dependencies;
		my @pendingNames = map { $_->name } @pending;

		# Include current entity in list of available entries, so that we can allow self-reference
		foreach my $dep (@deps) {
			unless(grep { $dep->name ~~ $_->name } @pending, @existing, $entity) {
				logError("%s unresolved (pending %s, deps %s for %s)", $dep->name, join(',', @pendingNames), join(',', @deps), $entity->name);
				die "Dependency error";
			}
		}

		my @unsatisfied = grep { $_ ~~ [ map { $_->name } @deps ] } @pendingNames;
		if(@unsatisfied) {
			logInfo("%s has %d unsatisfied deps, postponing: %s", $entity->name, scalar @unsatisfied, join(',',@unsatisfied));
			push @pending, $entity;
			next ITEM;
		}

		$self->apply_entity($entity);
		push @existing, $entity;
	}
	return $self;
}

=head2 apply_entity

Applies this entity to the database - currently, supports creation only.

=cut

sub apply_entity {
	my $self = shift;
	my $entity = shift;
	my ($sql, @bind) = $self->create_table_query($entity);

	my $sth = $self->dbh->prepare($sql);
	$sth->execute(@bind);
	$self->field_cache->clear;
	$self->primary_cache->clear;
	return $self;
}

=head2 read_primary

Get the primary keys for a table.

=cut

sub read_primary {
	my $self = shift;
	my $tbl = shift;
	logDebug("Get primary key info for [%s]", $tbl->name);
	$self->_cache_primary if $self->primary_cache->is_empty;

# Reorder the result according to the reported sequence
	my @keyList = map {
		$_->{name}
	} sort {
		$a->{order} <=> $b->{order}
	} grep {
		$_->{table} eq $tbl->name
	} $self->primary_cache->list;
	logDebug("Keys were: [%s]", join(',', @keyList));
	return @keyList;
}

=head2 read_fields

Read all fields for a given table.

Since this is typically a slow query, we cache the entire set of fields for all tables on
the first call.

=cut

sub read_fields {
	my $self = shift;
	my $tbl = shift;

	$self->_cache_fields unless $self->{field_cache};
	my $field_list = $self->field_cache->get($tbl->name);
	unless($field_list) {
		logDebug("No items for [%s]", $tbl->name);
		return;
	}
	logDebug("Check [%s] has %d items: %s", $tbl->name, $field_list->count, $field_list->join(','));
	return map +{ %$_ }, $field_list->list;
}

=head1 _cache_fields

Cache field information across all tables in the currently-selected database.

=cut

sub _cache_fields {
	my $self = shift;
	logInfo("Reloading cache");
# Get all field for all tables
	my $sth = $self->dbh->column_info(undef, $self->schema, '%', '%');
	my $rslt = $sth->fetchall_arrayref
		or return $self;
	my %field_cache;
	foreach (@$rslt) {
# We get a load of data back from DBI, most of which isn't useful yet
		my (undef, undef, $tableName, $name, $type, $size, $length, $digits, $radix, $nullable, $default, $dataType, $datetimeSub, $octetLength, $order, $isNullable) = @$_;
		$name =~ s/"//g;
		$tableName =~ s/"//g;
		logDebug("Have [%s] field [%s]", $tableName, $name);
		$field_cache{$tableName} = EntityModel::Array->new([ ]) unless $field_cache{$tableName};
		$field_cache{$tableName}->push({
			'name'		=> $name,
			'default'	=> $default,
			'null'		=> $isNullable,
			'type'		=> $type,
			'length'	=> $size,
			'precision'	=> $digits,
		});
	}
	$self->{field_cache} = \%field_cache;
	$self->_cache_primary;
	return $self;
}

=head1 _cache_primary

Cache primary key information across all tables in the database.

=cut

sub _cache_primary {
	my $self = shift;
	logInfo("Reloading primary cache");

# XXX PostgreSQL only code here, because DBI default was teh slow.
	my $sql = q{
select		'' as "something",
		n.nspname as "schema",
		c.relname as "table",
		a.attname as "column",
		a.attnum as "order",
		c2.relname as "keyname"
from		pg_catalog.pg_class c
inner join	pg_catalog.pg_index i on (i.indrelid = c.oid)
inner join	pg_catalog.pg_class c2 on (c2.oid = i.indexrelid)
inner join	pg_catalog.pg_attribute a on a.attrelid = c.oid and a.attnum = any(i.indkey)
inner join	pg_catalog.pg_type t2 on a.atttypid = t2.oid
left join	pg_catalog.pg_namespace n on (n.oid = c.relnamespace)
left join	pg_catalog.pg_tablespace t on (t.oid = c.reltablespace)
where		i.indisprimary is true
and		n.nspname = ?
order by	1,2,4
};
	my $sth = $self->dbh->prepare($sql);
	logDebug("Run $sql");
	$sth->execute($self->schema);
	# Get all tables
#	my $sth = $self->dbh->primary_key_info(undef, $self->schema, $tbl->name);
	my $rslt = $sth->fetchall_arrayref
		or return $self;

	my @keyList;
	foreach (@$rslt) {
		my (undef, undef, $tableName, $name, $order, $constraint) = @$_;
		push @keyList, {
			table => $tableName,
			name => $name,
			order => $order,
			constraint => $constraint
		};
	}
	logDebug("Had %d entries", scalar @keyList);
	$self->{primary_cache} = \@keyList;
	return $self;
}

=head2 table_list

Get a list of all the existing tables in the schema.

=cut

sub table_list {
	my $self = shift;
	my $q = EntityModel::Query->new(
		select	=> 'table_name',
		from	=> 'information_schema.tables',
		where	=> [
			table_type	=> 'BASE TABLE',
			table_schema	=> $self->schema,
		],
	);
	return $q->results;
}

=head2 field_list

Returns a list of all fields for the given table.

=cut

sub field_list {
	my $self = shift;
	my $tbl = shift;
	my $schema = $self->schema;

	my $q = EntityModel::Query->new(
		select	=> [
			{ name		=> 'column_name' },
			{ default	=> 'column_default' },
			{ null		=> 'is_nullable' },
			{ type		=> 'data_type' },
			{ length	=> 'character_maximum_length' },
			{ description	=> \q{''} },
			{ precision	=> 'numeric_precision' },
			{ scale		=> 'numeric_scale' },
		],
		from	=> { schema => 'information_schema', table => 'columns' },
		where	=> [
			table_schema	=> $schema,
		-and =>	table_name	=> $tbl,
		],
		order	=> 'ordinal_position'
	);
	return $q->results;
}

=head2 quoted_schema_name

Returns the quoted version of the current schema.

=cut

sub quoted_schema_name {
	my $self = shift;
	return undef unless $self->schema;

	return $self->dbh->quote_identifier($self->schema);
}

=head1 quoted_table_name

Generate the quoted table identifier including any schema name if available.

=cut

sub quoted_table_name {
	my $self = shift;
	my $tbl = shift;
	return $self->dbh->quote_identifier(undef, $self->schema, $tbl->name);
}

=head2 quoted_field_name

Generate the quoted field identifier.

=cut

sub quoted_field_name {
	my $self = shift;
	my $field = shift;
	return $self->dbh->quote_identifier(undef, undef, $field->name);
}

=head2 create_table_query

Create a new table.

=cut

sub create_table_query {
	my $self = shift;
	my $tbl = shift;

	my @bind;
	# Put together the constituent fields
	my $content = join(', ', map {
		$self->quoted_field_name($_) . ' ' . $_->type . ($tbl->primary eq $_->name ? ' primary key' : '')
	} $tbl->field->list);

	# TODO Any extras such as index or constraints

	# And build the create statement itself
	my $sql = 'create table ' . $self->quoted_table_name($tbl) . ' (' . $content . ')';
	return ($sql, @bind);
}

=head1 remove_table_query

Query for removing the given table.

=cut

sub remove_table_query {
	my $self = shift;
	my $tbl = shift;

	my @bind;
	my $sql = 'drop table ' . $self->quoted_table_name($tbl);
	return ($sql, @bind);
}

=head1 create_table

Create the given table.

=cut

sub create_table {
	my ($self, $tbl) = @_;
	my ($sql, @bind) = $self->create_table_query($tbl);
	my $sth = $self->dbh->prepare($sql);
	$sth->execute(@bind);
	$self->field_cache(undef);
	$self->primary_cache(undef);
	return $self->SUPER::create_table($tbl);
}

=head2 add_field_to_table

Add the requested field to the given table, and clear related caches.

=cut

sub add_field_to_table {
	my $self = shift;
	my $entity = shift;
	my $field = shift;

	my ($sql, @bind) = $self->alter_table_query(
		table	=> $entity,
		add	=> [ $field ]
	);
	my $sth = $self->dbh->prepare($sql);
	logDebug($sql);
	$sth->execute(@bind);
	$self->field_cache(undef);
	$self->primary_cache(undef);
	return $self->SUPER::add_field_to_table($entity, $field);
}

=head2 remove_table

Remove a table entirely.

=cut

sub remove_table {
	my $self = shift;
	my $tbl = shift;

	my ($sql, @bind) = $self->remove_table_query($tbl);
	my $sth = $self->dbh->prepare($sql);
	logInfo($sql);
	$sth->execute(@bind);
	$self->field_cache(undef);
	$self->primary_cache(undef);
	return $self;
}

=head2 read_tables

Read all table definitions from the database.

=cut

sub read_tables {
	my $self = shift;
	die 'no schema' unless $self->schema;
	logWarning("Get tables for " . $self->schema);

	delete $self->{field_cache};
	$self->primary_cache(undef);
	my $sth = $self->dbh->table_info(undef, $self->schema, '%');
	my $rslt = $sth->fetchall_arrayref
		or return $self;
	my @table_list;
	foreach (@$rslt) {
		my (undef, undef, $name, $type) = @$_;
		$name =~ s/^"//;
		$name =~ s/"$//;
		push @table_list, { name => $name } if lc($type) eq 'table';
	}
	return @table_list;	
}

=head2 post_commit

=cut

sub post_commit {
	my $self = shift;
	$self->dbh->commit;
	return $self;
}

=head2 create_schema

=cut

sub create_schema {
	my $self = shift;
	try {
		$self->db->transaction(sub {
			my $dbh = shift->dbh;
			$dbh->do('create schema ' . $self->quoted_schema_name);
		});
	} catch {
		logWarning($_);
	};
	return $self;
}

=head1 remove_schema

Remove the schema entry.

=cut

sub remove_schema {
	my $self = shift;
	die "No schema" unless $self->schema;
	try {
		$self->db->transaction(sub {
			my $dbh = shift->dbh;
			$dbh->do('drop schema ' . $self->quoted_schema_name . ' cascade');
		});
	} catch {
		logWarning($_);
	};
	return $self;
}

=head2 schema_exists

Returns true if the current schema exists in the database, false if not.

=cut

sub schema_exists {
	my $self = shift;
	my $sth = $self->dbh->prepare(q{select schema_name, catalog_name from information_schema.schemata where schema_name = ?});
	$sth->execute($self->schema);
	my $rslt = $sth->fetchall_arrayref
		or return undef;
	return scalar @$rslt;
}

=head2 row_count

Reports how many rows are in the given table.

=cut

sub row_count {
	my $self = shift;
	my $tbl = shift;
	die 'not yet implemented';
}

=head2 find

Find entries.

=cut

sub find {
	my $self = shift;
	my $tbl = shift;
	my $spec = shift;
	die 'not yet implemented';
}

=head2 create

Creates a new instance for the given entity.

=cut

sub create {
	my $self = shift;
	my %args = @_;
	logError("Creating entity [%s] with [%s]", $args{entity}, $args{data});
	my $q = EntityModel::Query->new(
		db		=> $self->db,
		'insert into'	=> $self->quoted_table_name($args{entity}),
		values		=> $args{data},
		returning	=> [ $args{entity}->primary ]
	);
	my ($rslt) = $q->results;
	return $rslt->{$args{entity}->primary};
}

=head2 store

Update the database with current in-memory values for the given entity instance.

=cut

sub store {
	my $self = shift;
	my %args = @_;
	logError("Creating entity [%s] with [%s]", $args{entity}, $args{data});
	my $q = EntityModel::Query->new(
		db		=> $self->db,
		'update'	=> $self->quoted_table_name($args{entity}),
		fields		=> $args{data},
		where		=> [ $args{entity}->primary => $args{id} ]
	);
	my $rslt = $q->results;
	return $rslt;
}

=head2 read

Read information for the requested entity instance.

=cut

sub read {
	my $self = shift;
	my %args = @_;
	logDebug("Reading entity [%s] id [%s]", $args{entity}, $args{id});
	my $q = EntityModel::Query->new(
		db		=> $self->db,
		'select'	=> [ map { $self->quoted_field_name($_) } $args{entity}->field->list ],
		'from'		=> $self->quoted_table_name($args{entity}),
		where		=> [ $args{entity}->primary => $args{id} ],
		limit		=> 1
	);
	my ($rslt) = $q->results;
	logError($rslt);
	return $rslt;
}

=head2 dbh

Returns a database handle for this storage backend.

=cut

sub dbh {
	my $self = shift;
	return $self->db->dbh(@_);
}

1;

__END__

=head1 AUTHOR

Tom Molesworth <cpan@entitymodel.com>

=head1 LICENSE

Copyright Tom Molesworth 2008-2011. Licensed under the same terms as Perl itself.

