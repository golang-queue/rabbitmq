package rabbitmq

import "testing"

func Test_isVaildExchange(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "fanout",
			args: args{
				name: ExchangeFanout,
			},
			want: true,
		},
		{
			name: "invaild exchange type",
			args: args{
				name: "test",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isVaildExchange(tt.args.name); got != tt.want {
				t.Errorf("isVaildExchange() = %v, want %v", got, tt.want)
			}
		})
	}
}
