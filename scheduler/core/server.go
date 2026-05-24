package core

import (
	"context"
	"log"

	pb "github.com/chorus/proto"
	"github.com/chorus/scheduler/job"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedSchedulerServiceServer
	sched *Scheduler
}

func NewServer(s *Scheduler) *Server {
	return &Server{sched: s}
}

func (s *Server) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "job ID is required")
	}
	if req.Cost <= 0 {
		return nil, status.Error(codes.InvalidArgument, "cost must be positive")
	}

	j := &job.Job{
		ID:       req.Id,
		Priority: int(req.Priority),
		Cost:     int(req.Cost),
		OutputCh: make(chan string),
		JobType:  req.JobType,
	}

	if err := s.sched.Submit(j); err != nil {
		log.Printf("SubmitJob id=%s rejected: %v", req.Id, err)
		return &pb.SubmitJobResponse{
			Id:     req.Id,
			Status: "rejected",
			Reason: err.Error(),
		}, nil
	}

	log.Printf("SubmitJob id=%s priority=%d cost=%d", req.Id, req.Priority, req.Cost)

	return &pb.SubmitJobResponse{
		Id:     req.Id,
		Status: "pending",
	}, nil
}

func (s *Server) RunJob(req *pb.RunJobRequest, stream grpc.ServerStreamingServer[pb.RunJobResponse]) error {
	if req.Id == "" {
		return status.Error(codes.InvalidArgument, "job ID is required")
	}

	j := &job.Job{
		ID:       req.Id,
		Priority: int(req.Priority),
		Cost:     int(req.Cost),
		OutputCh: make(chan string),
		JobType:  req.JobType,
	}

	if err := s.sched.Submit(j); err != nil {
		return status.Errorf(codes.ResourceExhausted, "rejected: %v", err)
	}

	stream.Send(&pb.RunJobResponse{
		JobId: req.Id,
		Event: &pb.RunJobResponse_Queued{Queued: &pb.JobQueued{}},
	})

	for token := range j.OutputCh {
		stream.Send(&pb.RunJobResponse{
			JobId: req.Id,
			Event: &pb.RunJobResponse_Chunk{Chunk: &pb.OutputChunk{Token: token}},
		})
	}

	if j.Err == nil {
		stream.Send(&pb.RunJobResponse{
			JobId: req.Id,
			Event: &pb.RunJobResponse_Completed{Completed: &pb.JobCompleted{}},
		})
	} else {
		stream.Send(&pb.RunJobResponse{
			JobId: req.Id,
			Event: &pb.RunJobResponse_Failed{Failed: &pb.JobFailed{}},
		})
	}

	return nil
}

func (s *Server) GetJobStatus(ctx context.Context, req *pb.JobStatusRequest) (*pb.JobStatusResponse, error) {
	j, ok := s.sched.GetJob(req.Id)
	if !ok {
		return &pb.JobStatusResponse{
			Id:     req.Id,
			Status: "unknown",
		}, nil
	}

	return &pb.JobStatusResponse{
		Id:       j.ID,
		Status:   j.Status.String(),
		WorkerId: j.WorkerID,
		Priority: int32(j.Priority),
		Cost:     int32(j.Cost),
	}, nil
}

func (s *Server) ListJobs(ctx context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	jobs := s.sched.GetAllJobs()
	summaries := make([]*pb.JobSummary, len(jobs))
	for i, j := range jobs {
		summaries[i] = &pb.JobSummary{
			Id:       j.ID,
			Status:   j.Status.String(),
			Priority: int32(j.Priority),
			Cost:     int32(j.Cost),
			WorkerId: j.WorkerID,
		}
	}

	return &pb.ListJobsResponse{
		Jobs: summaries,
	}, nil
}
