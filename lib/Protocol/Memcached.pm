package Protocol::Memcached;
# ABSTRACT: Support for the memcached binary protocol
use strict;
use warnings FATAL => 'all';

our $VERSION = '0.003';

=head1 NAME

Protocol::Memcached - memcached binary protocol implementation

=head1 VERSION

version 0.003

=head1 SYNOPSIS

 package Subclass::Of::Protocol::Memcached;
 use parent qw(Protocol::Memcached);

 sub write { $_[0]->{socket}->write($_[1]) }

 package main;
 my $mc = Subclass::Of::Protocol::Memcached->new;
 my ($k, $v) = ('hello' => 'world');
 $mc->set(
 	$k => $v,
	on_complete	=> sub {
 		$mc->get(
 			'key',
			on_complete	=> sub { my $v = shift; print "Had $v\n" },
			on_error	=> sub { die "Failed because of @_\n" }
 		);
	}
 );

=head1 DESCRIPTION

Bare minimum protocol support for memcached. This class is transport-agnostic and as
such is not a working implementation - you need to subclass and provide your own ->write
method.

If you're using this class, you're most likely doing it wrong - head over to the
L</SEE ALSO> section to rectify this.

L<Protocol::Memcached::Client> is probably the module you want if you are going to subclass
this.

=head1 SUBCLASSING

Provide the following method:

=head2 write

This will be called with the data to be written, and zero or more named parameters:

=over 4

=item * on_flush - coderef to execute when the data has left the building, if this is
not supported by the transport layer then the subclass should call the coderef
before returning

=back

and when you have data, call L</on_read>.

=cut

# Modules

use Scalar::Util ();

# Constants

use constant {
	MAGIC_REQUEST => 0x80,
	MAGIC_RESPONSE => 0x81,
};

# Mapping from numeric opcode value in packet header to method
my %OPCODE_BY_ID = (
	0x00 => 'Get',
	0x01 => 'Set',
	0x02 => 'Add',
	0x03 => 'Replace',
	0x04 => 'Delete',
	0x05 => 'Increment',
	0x06 => 'Decrement',
	0x07 => 'Quit',
	0x08 => 'Flush',
	0x09 => 'GetQ',
	0x0A => 'No-op',
	0x0B => 'Version',
	0x0C => 'GetK',
	0x0D => 'GetKQ',
	0x0E => 'Append',
	0x0F => 'Prepend',
	0x10 => 'Stat',
	0x11 => 'SetQ',
	0x12 => 'AddQ',
	0x13 => 'ReplaceQ',
	0x14 => 'DeleteQ',
	0x15 => 'IncrementQ',
	0x16 => 'DecrementQ',
	0x17 => 'QuitQ',
	0x18 => 'FlushQ',
	0x19 => 'AppendQ',
	0x1A => 'PrependQ',
);
# Map from method name to opcode byte
my %OPCODE_BY_NAME = reverse %OPCODE_BY_ID;

# Status values from response
my %RESPONSE_STATUS = (
	0x0000 => 'No error',
	0x0001 => 'Key not found',
	0x0002 => 'Key exists',
	0x0003 => 'Value too large',
	0x0004 => 'Invalid arguments',
	0x0005 => 'Item not stored',
	0x0006 => 'Incr/Decr on non-numeric value',
	0x0081 => 'Unknown command',
	0x0082 => 'Out of memory',
);

=head1 METHODS

=cut

=head2 new

Bare minimum constructor - subclass may need to inherit from something with a
non-trivial constructor, so we put all our init code in L</init>.

=cut

sub new {
	my $class = shift;
	my $self = bless { }, $class;
	return $self;
}

=head2 sap

Helper method for weak callbacks.

=cut

sub sap { my ($self, $sub) = @_; Scalar::Util::weaken $self; return sub { $self->$sub(@_); }; }

=head2 get

Retrieves a value from memcached.

Takes a key and zero or more optional named parameters:

=over 4

=item * on_write - called when we've sent the request to the server

=back

=cut

sub get {
	my $self = shift;
	my $k = shift; # FIXME should we do anything about encoding or length checks here?
	my %args = @_;

# Pull out any callbacks that we handle directly
	my $on_write = delete $args{on_write};

	my $len = length $k; # TODO benchmark - 2xlength calls or lexical var?

	$self->write(
		pack(
			'C1 C1 n1 C1 C1 n1 N1 N1 N1 N1 a*',
			MAGIC_REQUEST,		# What type this packet is
			$OPCODE_BY_NAME{'Get'},	# Opcode
			$len,			# Key length
			0x00,			# Extras length
			0x00,			# Data type binary
			0x0000,			# Reserved
			$len,			# Total body
			0x00000000,		# Opaque
			0x00,			# CAS
			0x00,			# more CAS - 8byte value but don't want to rely on pack 'Q'
			$k,
		),
		on_flush => $self->sap(sub {
			my $self = shift;
			push @{ $self->{pending} }, {
				%args,
				type	=> 'Get',
				key	=> $k,
			};
			$on_write->($self, key => $k) if $on_write;
		})
	);
	$self
}

=head2 set

Retrieves a value from memcached.

Takes a key and zero or more optional named parameters:

=over 4

=item * on_write - called when we've sent the request to the server

=back

=cut

sub set {
	my $self = shift;
	my $k = shift; # FIXME should we do anything about encoding or length checks here?
	my $v = shift;
	my %args = @_;

# Pull out any callbacks that we handle directly
	my $on_write = delete $args{on_write};

	$self->write(
		pack(
			'C1 C1 n1 C1 C1 n1 N1 N1 N1 N1 N1 N1 a* a*',
			MAGIC_REQUEST,		# What type this packet is
			$OPCODE_BY_NAME{'Set'},	# Opcode
			length($k),		# Key length
			0x08,			# Extras length
			0x00,			# Data type binary
			0x0000,			# Reserved
			8 + length($k) + length($v),			# Total body
			0x00000000,		# Opaque
			0x00,			# CAS
			0x00,			# more CAS - 8byte value but don't want to rely on pack 'Q'
			$args{flags} || 0,
			$args{ttl} || 60,
			$k,
			$v,
		),
		on_flush => $self->sap(sub {
			my $self = shift;
			push @{ $self->{pending} }, {
				%args,
				type	=> 'Set',
				key	=> $k,
				value	=> $v,
			};
			$on_write->($self, key => $k, value => $v) if $on_write;
		})
	);
	$self
}

=head2 init

=cut

sub init {	
	my $self = shift;
	$self->{pending} = [];
	return $self;
}

=head2 on_read

This should be called when there is data to be processed. It takes a single parameter:
a reference to a buffer containing the incoming data. If a packet is processed
successfully then it will be removed from this buffer (via C< substr > or C< s// >).

Returns true if a packet was found, false if not. It is recommended (but not required)
that this method be called repeatedly until it returns false.

=cut

sub on_read {
	my ($self, $buffref) = @_;

# Bail out if we don't have a full header
	return 0 unless length $$buffref >= 24;

# Extract the basic header data first - specifically we want the length
	my ($magic, $opcode, $kl, $el, $dt, $status, $blen, $opaque, $cas1, $cas2) = unpack('C1 C1 n1 C1 C1 n1 N1 N1 N1 N1', $$buffref);
	die "Not a response" unless $magic == MAGIC_RESPONSE;

# If we don't have the full body as well, bail out here
	return 0 unless length $$buffref >= ($blen + 24);

# Strip the header
	substr $$buffref, 0, 24, '';

	my $body = substr $$buffref, 0, $blen, '';
	if($opcode == 0x00) {
		my $flags = substr $body, 0, 4, '';
	}
#	printf "=> %-9.9s %-40.40s %08x%08x %s\n", $OPCODE_BY_ID{$opcode}, $body, $cas1, $cas2, $RESPONSE_STATUS{$status} // 'unknown status';
	my $item = shift @{$self->{pending}} or die "Had response with no queued item\n";
	$item->{value} = $body if length $body;
	if($status) {
		$item->{on_error}->(%$item, status => $status) if exists $item->{on_error};
		die "Failed with " . $RESPONSE_STATUS{$status} . " on item " . join ',', %$item . "\n";
	} else {
		$item->{on_complete}->(%$item) if exists $item->{on_complete};
	}
	return 1;
}

=head2 build_packet

Generic packet construction.

=cut

sub build_packet {
	my $self = shift;
	my %args = @_;
	my $pkt = pack(
		'C1 C1 S1 C1 C1 S1 N1 N1 N1',
		$args{request} ? MAGIC_REQUEST : MAGIC_RESPONSE,
		$args{opcode},
		defined($args{key}) ? length($args{key}) : 0,
		defined($args{extras}) ? length($args{extras}) : 0,
		0x00,
		defined($args{body}) ? length($args{body}) : 0,
		0x00,
		0x00
	);
	return $pkt;
}

1;

__END__

=head1 WHY

Three main reasons:

=over 4

=item * B<Transport-agnostic> - purposefully does B< not > get involved in the details of
sending or receiving data, when it wants to write something it'll call L<write>, and
when you have data to process you call L<on_read>

=item * B<Nonblocking> - since this just operates on data and callbacks, rather than getting
involved in transporting data, all operations should return quickly (in Perl terms)

=item * B<Debugging support> - strap this over a memcached transport layer and see
human-readable versions of the binary packets

=back

If you're looking for good performance, stability, an extensive set of tests, support,
and a pony, then you're reading the wrong module:

=head1 SEE ALSO

=over 4

=item * L<Cache::Memcached> - official implementation

=item * L<AnyEvent::Memcached> - text protocol support for L<AnyEvent>

=item * L<Cache::Memcached::AnyEvent> - provides binary protocol support for L<AnyEvent>

=item * L<Memcached::Client> - another L<AnyEvent> implementation, again with the
transport layer too highly coupled for my purposes

=item * L<Cache::Memcached::GetParserXS> - XS implementation for parsing memcached
binary data, apparently "possibly twice as fast as the original perl version".

=back

=head1 AUTHOR

Tom Molesworth <cpan@entitymodel.com>

=head1 LICENSE

Copyright Tom Molesworth 2011. Licensed under the same terms as Perl itself.
