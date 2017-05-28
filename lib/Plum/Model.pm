use strict;
use warnings;
use v5.16;

package Plum::Model;

use Carp qw(confess);
use Encode;
use JSON;

my $JSON = JSON->new->utf8;

sub new {
    my $proto = shift;
    my $self = bless {@_}, ref($proto)||$proto;
    return $self->init;
}

sub init {
    return $_[0];
}

sub table {
    die "override table method";
}

sub unique_sets {
    return (['id']);
}

sub load {
    my $self = shift;
    my %args = @_;

    $self = $self->new unless ref $self;

    my $dbh = delete $args{dbh} or confess("dbh required");
    my $for_update = delete $args{for_update};

    my $query = 'SELECT * FROM '. $self->table .' WHERE ';

    my (@parts, @binds);
    while ( my ($k, $v) = each %args ) {
        if ( defined $v ) {
            push @parts, "$k = ?";
            push @binds, $v;
        } else {
            push @parts, "$k IS NULL";
        }
    }
    $query .= join ' AND ', @parts;
    $query .= ' FOR UPDATE' if $for_update;

    return $dbh->selectrow_hashref(
        $query, undef, @binds
    )
    ->then(sub {
        print STDERR "WTF? $query @binds\n" unless defined $_[0];
        return $self->expand_from_row_data( shift );
    });
}

sub create {
    my $self = shift;
    my %args = @_;

    $self = $self->new unless ref $self;

    my $dbh = delete $args{dbh};
    my @cols = keys %args;
    my $query = q{INSERT INTO }. $self->table;
    if ( @cols ) {
        $query .= '('. join( ', ', keys %args ) .q{) VALUES (}. join( ',', ('?')x keys %args ) .')'
    } else {
        $query .= ' VALUES (DEFAULT)';
    }
    $query .= q{ RETURNING *};

    my $values = $self->format_row_data( \%args, \@cols );

    return $dbh->selectrow_hashref(
        $query, undef, @$values,
    )->then(sub {
        return $self->expand_from_row_data( shift );
    })
}

sub remove {
    my $self = shift;
    my %args = @_;

    my $by;
    foreach my $set ( $self->unique_sets ) {
        next if grep !exists $args{$_} || !defined $args{$_}, @$set;
        $by = $set; last;
    }
    die $self->exception('required', field => 'one of unique sets') unless $by;

    my $dbh = delete $args{dbh};
    my $query = 'DELETE FROM '. $self->table
        .' WHERE '. join(' AND ', map "$_ = ?", @$by )
    ;
    return $dbh->do(
        $query, undef, @args{ @$by }
    )
    ->then(sub {
        return shift;
    });
}

sub store {
    my $self = shift;
    my %args = @_;

    my @dirty = keys %{ $self->{dirty}||{} };
    return $self->u->deferred->resolve( $self )->promise
        unless @dirty;

    my $by;
    foreach my $set ( $self->unique_sets ) {
        next if grep $self->{dirty}{$_}, @$set;
        next if grep !exists $self->{values}{$_} || !defined $self->{values}{$_}, @$set;
        $by = $set; last;
    }
    die $self->exception('dirty', field => 'one of unique sets') unless $by;

    return $self->_update(
        dbh => $args{dbh},
        from => $self->{values},
        by => $by,
        what => \@dirty
    );
}

sub update {
    my $self = shift;
    my %args = @_;

    $self = $self->new unless ref $self;

    my $by;
    foreach my $set ( $self->unique_sets ) {
        next if grep !exists $args{$_} || !defined $args{$_}, @$set;
        $by = $set; last;
    }
    die $self->exception('required', field => 'one of unique sets') unless $by;

    my $dbh = delete $args{dbh};
    my @what;
    foreach my $k ( keys %args ) {
        next if grep $k eq $_, @$by;
        push @what, $k;
    }

    return $self->_update(
        dbh => $dbh, from => \%args, by => $by, what => \@what,
    );
}

sub _update {
    my $self = shift;
    my %args = (
        dbh => undef,
        @_
    );

    my $dbh = $args{dbh} or confess "dbh required";

    my $binds = $self->format_row_data( $args{from}, [@{$args{what}}, @{$args{by}}] );
    my $query = 'UPDATE '. $self->table
        .' SET '. join(', ', map "$_ = ?", @{ $args{what} })
        .' WHERE '. join(' AND ', map "$_ = ?", @{ $args{by} } )
        .' RETURNING *'
    ;
    return $args{dbh}->selectrow_hashref(
        $query, undef, @$binds
    )->then(sub {
        return $self->expand_from_row_data( shift );
    })
}

sub simple_find {
    my $self = shift;
    my %args = @_;

    my $dbh = delete $args{dbh};
    my $query = $args{query};
    unless ( $query ) {
        $query = 'SELECT t.* FROM '. $self->table .' t';
    }
    if ( $args{columns} && %{ $args{columns} } ) {
        my $where = join ' AND ', map "$_ = ?", keys %{$args{columns}};
        push @{ $args{binds}||= [] }, values %{$args{columns}};

        if ( $args{where} ) {
            $args{where} .= " AND ($where)";
        } else {
            $args{where} = $where;
        }
    }
    if ( $args{where} ) {
        $query .= ' WHERE '. $args{where};
    }
    if ( $args{order} ) {
        $query .= ' ORDER BY '. $args{order};
    }
    if ( $args{limit} ) {
        $query .= ' LIMIT '. int $args{limit};
    }
    return $dbh->selectall_arrayref(
        $query, { Slice => {} }, @{ $args{binds} || [] },
    )->then(sub {
        my $rows = shift;
        foreach my $row ( @$rows ) {
            $row = $self->new->expand_from_row_data( $row );
        }
        return $rows;
    });
}

sub expand_from_row_data {
    my $self = shift;
    my $row = shift or return $self;
    confess("Not a reference") unless ref $row;

    my $s = $self->structure;
    foreach my $k ( keys %$row ) {
        next unless defined $row->{$k};
        next unless my $type = $s->{$k}{type};

        if ( $type eq 'json' ) {
            Encode::_utf8_off($row->{$k});
            $row->{$k} = $JSON->decode( $row->{$k} );
        }
        elsif ( $type eq 'timestamp' ) {
            $row->{$k} =~ s{
                ^([0-9]{4}-[0-9]{2}-[0-9]{2})\s+([0-9]{2}:[0-9]{2}:)([0-9]{2}(?:\.[0-9]+)?)([+-][0-9:]+|Z)$
            }{
                "$1T$2". sprintf("%.3f", $3) ."$4"
            }xe or die "Unexpected timestamp format from Pg: '". $row->{$k} ."'";
        }
    }
    $self->{dirty} = {};
    $self->{values} = $row;
    return $self;
}

sub format_row_data {
    my $self = shift;
    my $from = shift;
    my $columns = shift;

    my $s = $self->structure;

    my @values;
    foreach my $f ( @$columns ) {
        my $type = $s->{$f}{type};
        my $v = $from->{ $f };
        if ( !defined $v || !$type ) {
            push @values, $v;
        }
        elsif ( $type eq 'json' ) {
            $v = $JSON->encode($v);
            Encode::_utf8_on($v);
            push @values, $v;
        }
        elsif ( $type eq 'timestamp' ) {
            push @values, $v;
        }
        else {
            die "No formatter for column type '$type'";
        }
    }
    return \@values;
}

sub generate_accessors {
    my $class = shift;

    my $s = $class->structure;
    while ( my ($method, $info) = each %$s ) {
        next if $class->can( $method );

        no strict qw(refs subs);
        *{ $class .'::'. $method } = sub {
            my $self = shift;
            if ( @_ ) {
                $self->{values}{$method} = shift;
                $self->{dirty}{$method} = 1;
                return $self;
            } else {
                return $self->{values}{$method};
            }
        };
    }
}

1;
