package mq

import (
	"reflect"
)

type DataStream struct {
	mq    MessageQueue
	data  DataValue
	datat reflect.Type
}

type DataValue interface {
	Unmarshal([]byte) error
	Marshal() ([]byte, error)
	Consume() error
}

func NewDataStream(mq MessageQueue, dataptr DataValue) *DataStream {
	return &DataStream{
		mq:    mq,
		data:  dataptr,
		datat: reflect.TypeOf(dataptr),
	}
}

func (ds *DataStream) SendMessage(data DataValue, opts ...SendMsgOption) error {
	bytevalue, err := data.Marshal()
	if err != nil {
		return err
	}
	return ds.mq.SendMessage(bytevalue, opts...)
}

func (ds *DataStream) Consume(opts ...ConsumeMsgOption) error {
	err := ds.mq.ConsumeMessage(ds.ConsumeMessageFunc, opts...)
	return err
}

func (ds *DataStream) ConsumeMessageFunc(msg Message) {
	var err error
	newdata := reflect.New(ds.datat).Interface().(DataValue)
	body := msg.Body()
	err = newdata.Unmarshal(body)
	if err != nil {
		msg.Nack()
		return
	}
	err = newdata.Consume()
	if err != nil {
		msg.Nack()
		return
	}
	msg.Ack()
}
