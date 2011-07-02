#!/usr/bin/perl 
use strict;
use warnings;
use 5.010;
#use Protocol::Memcached;

eval { require IO::Async::Loop; 1; } or die "This example needs IO::Async\n";

my $loop = IO::Async::Loop->new;
$loop->connect(
	host		=> 'localhost',
	service 	=> 11211,
	socktype	=> 'stream',
	on_stream	=> sub {
		my $stream = shift;
		warn "Connected to memcached\n";
		$stream->configure(
			on_read => sub {
				my ($self, $buffref, $eof) = @_;
				if(length $$buffref >= 24) {
					my ($magic, $opcode, $kl, $el, $dt, $status, $blen, $opaque, $cas1, $cas2) = unpack('C1 C1 n1 C1 C1 n1 N1 N1 N1 N1', $$buffref);
					substr $$buffref, 0, 24, '';
#					warn "Had " . join(',', map { sprintf '%08x', $_ } $magic, $opcode, $kl, $el, $dt, $status, $blen, $opaque, $cas1, $cas2) . "\n";
					die "Not a response" unless $magic == 0x81;
					my $body = substr $$buffref, 0, $blen, '';
					if($opcode == 0x00) {
						my $flags = substr $body, 0, 4, '';
					}
					printf "=> %-9.9s %-40.40s %08x%08x %s\n", $OPCODE_MAP{$opcode}, $body, $cas1, $cas2, $RESPONSE_STATUS{$status} // 'unknown status';
					return 1;
				}
				return undef;
			}
		);
		$loop->add($stream);

		my ($k, $v) = ('Hello', 'World');
		$stream->write(
			pack(
				'C1 C1 n1 C1 C1 n1 N1 N1 N1 N1 a*',
				0x80,		# Request
				0x04,		# Opcode - DELETE
				length($k),	# Key length
				0x00,		# Extras length
				0x00,		# Data type
				0x0000,		# Reserved
				length($k),	# Total body
				0x00000000,	# Opaque
				0x00,		# CAS
				0x00,		# CAS
				$k,
			),
			on_flush => sub {
				warn "DELETE [$k]\n";
			}
		);

		$stream->write(
			pack(
				'C1 C1 n1 C1 C1 n1 N1 N1 N1 N1 N1 N1 a* a*',
				0x80,		# Request
				$OPCODE_REVMAP{'Set'},
#				0x02,		# Opcode - SET
				length($k),	# Key length
				0x08,		# Extras length
				0x00,		# Data type
				0x0000,		# Reserved
				8 + length($k) + length($v),
#				0x00000012,	# Total body
				0x00000000,
				0x00, # CAS
				0x00,
				0xDEADBEEF,
				0x00000E10,
				$k,
				$v,
			),
			on_flush => sub {
				warn "SET    [$k] => [$v]\n"
			}
		);

		$stream->write(
			pack(
				'C1 C1 n1 C1 C1 n1 N1 N1 N1 N1 a*',
				0x80,			# Request
				$OPCODE_REVMAP{'Get'},	# Opcode - GET
				length($k),		# Key length
				0x00,			# Extras length
				0x00,			# Data type
				0x0000,			# Reserved
				length($k),		# Total body
				0x00000000,		# Opaque
				0x00,			# CAS
				0x00,			# more CAS
				$k,
			),
			on_flush => sub {
				warn "GET    [$k] => [$v]\n"
			}
		);
	},
        on_resolve_error => sub { die "Cannot resolve - $_[-1]\n"; },
        on_connect_error => sub { die "Cannot connect - $_[0] failed $_[-1]\n"; },
);
$loop->loop_forever;

