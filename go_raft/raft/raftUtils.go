package raft

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/binary"

	log "github.com/sirupsen/logrus"
)

func checkError(err error) {
	if err != nil {
		log.Error("Error: ", err)
	}
}

func getRaftLogBytes(rl RaftLog) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, rl.Idx)
	binary.Write(buf, binary.LittleEndian, rl.Term)
	binary.Write(buf, binary.LittleEndian, rl.Type)
	binary.Write(buf, binary.LittleEndian, rl.Log.Id)
	binary.Write(buf, binary.LittleEndian, rl.Log.ActionId)
	binary.Write(buf, binary.LittleEndian, rl.Log.Type)
	binary.Write(buf, binary.LittleEndian, rl.Log.Action)
	binary.Write(buf, binary.LittleEndian, rl.ConfigurationLog.Add)
	binary.Write(buf, binary.LittleEndian, rl.ConfigurationLog.Server)
	return buf.Bytes()
}

func getAppendEntriesArgsBytes(aea *AppendEntriesArgs) [32]byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, (*aea).Term)
	binary.Write(buf, binary.LittleEndian, (*aea).LeaderID)
	binary.Write(buf, binary.LittleEndian, (*aea).PrevLogIndex)
	binary.Write(buf, binary.LittleEndian, (*aea).PrevLogTerm)
	binary.Write(buf, binary.LittleEndian, (*aea).PrevLogHash)
	binary.Write(buf, binary.LittleEndian, (*aea).Entries)
	binary.Write(buf, binary.LittleEndian, (*aea).LeaderCommit)
	binary.Write(buf, binary.LittleEndian, (*aea).CurrentVotes)

	return sha256.Sum256(buf.Bytes())
}

func getAppendEntriesArgsSignature(privKey *rsa.PrivateKey, aea *AppendEntriesArgs) []byte {
	hashed := getAppendEntriesArgsBytes(aea)
	signature, err := rsa.SignPKCS1v15(rand.Reader, privKey, crypto.SHA256, hashed[:])
	checkError(err)

	return signature
}

func getAppendEntriesResponseBytes(aer *AppendEntriesResponse) [32]byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, (*aer).Id)
	binary.Write(buf, binary.LittleEndian, (*aer).Term)
	binary.Write(buf, binary.LittleEndian, (*aer).Success)
	binary.Write(buf, binary.LittleEndian, (*aer).LastIndex)

	return sha256.Sum256(buf.Bytes())
}

func getAppendEntriesResponseSignature(privKey *rsa.PrivateKey, aer *AppendEntriesResponse) []byte {
	hashed := getAppendEntriesResponseBytes(aer)
	signature, err := rsa.SignPKCS1v15(rand.Reader, privKey, crypto.SHA256, hashed[:])
	checkError(err)

	return signature
}

func getRequestVoteArgsBytes(rva *RequestVoteArgs) [32]byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, (*rva).Term)
	binary.Write(buf, binary.LittleEndian, (*rva).CandidateID)
	binary.Write(buf, binary.LittleEndian, (*rva).LastLogIndex)
	binary.Write(buf, binary.LittleEndian, (*rva).LastLogTerm)

	return sha256.Sum256(buf.Bytes())
}

func getRequestVoteArgsSignature(privKey *rsa.PrivateKey, rva *RequestVoteArgs) []byte {
	hashed := getRequestVoteArgsBytes(rva)
	signature, err := rsa.SignPKCS1v15(rand.Reader, privKey, crypto.SHA256, hashed[:])
	checkError(err)

	return signature
}

func getRequestVoteResponseBytes(rvr *RequestVoteResponse) [32]byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, (*rvr).Id)
	binary.Write(buf, binary.LittleEndian, (*rvr).Term)
	binary.Write(buf, binary.LittleEndian, (*rvr).VoteGranted)

	return sha256.Sum256(buf.Bytes())
}

func getRequestVoteResponseSignature(privKey *rsa.PrivateKey, rvr *RequestVoteResponse) []byte {
	hashed := getRequestVoteResponseBytes(rvr)
	signature, err := rsa.SignPKCS1v15(rand.Reader, privKey, crypto.SHA256, hashed[:])
	checkError(err)

	return signature
}

func getInstallSnapshotArgsBytes(isa *InstallSnapshotArgs) [32]byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, (*isa).Id)
	binary.Write(buf, binary.LittleEndian, (*isa).Term)
	binary.Write(buf, binary.LittleEndian, (*isa).LastIncludedIndex)
	binary.Write(buf, binary.LittleEndian, (*isa).LastIncludedTerm)
	binary.Write(buf, binary.LittleEndian, (*isa).Data)
	binary.Write(buf, binary.LittleEndian, (*isa).ServerConfiguration)
	binary.Write(buf, binary.LittleEndian, (*isa).Hash)

	return sha256.Sum256(buf.Bytes())
}

func getInstallSnapshotArgsSignature(privKey *rsa.PrivateKey, isa *InstallSnapshotArgs) []byte {
	hashed := getInstallSnapshotArgsBytes(isa)
	signature, err := rsa.SignPKCS1v15(rand.Reader, privKey, crypto.SHA256, hashed[:])
	checkError(err)

	return signature
}

func getInstallSnapshotResponseBytes(isr *InstallSnapshotResponse) [32]byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, (*isr).Id)
	binary.Write(buf, binary.LittleEndian, (*isr).Term)
	binary.Write(buf, binary.LittleEndian, (*isr).Success)
	binary.Write(buf, binary.LittleEndian, (*isr).LastIncludedIndex)
	binary.Write(buf, binary.LittleEndian, (*isr).LastIncludedTerm)

	return sha256.Sum256(buf.Bytes())
}

func getInstallSnapshotResponseSignature(privKey *rsa.PrivateKey, isr *InstallSnapshotResponse) []byte {
	hashed := getInstallSnapshotResponseBytes(isr)
	signature, err := rsa.SignPKCS1v15(rand.Reader, privKey, crypto.SHA256, hashed[:])
	checkError(err)

	return signature
}

func GetActionArgsBytes(aa *ActionArgs) [32]byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, (*aa).Id)
	binary.Write(buf, binary.LittleEndian, (*aa).ActionId)
	binary.Write(buf, binary.LittleEndian, (*aa).Type)
	binary.Write(buf, binary.LittleEndian, (*aa).Action)
	return sha256.Sum256(buf.Bytes())
}

func GetConfigurationLogBytes(cl ConfigurationLog) [32]byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, cl.Add)
	binary.Write(buf, binary.LittleEndian, cl.Server)
	return sha256.Sum256(buf.Bytes())
}

func GetAddRemoveServerArgsBytes(ar *AddRemoveServerArgs) [32]byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, (*ar).Add)
	binary.Write(buf, binary.LittleEndian, (*ar).Server)
	return sha256.Sum256(buf.Bytes())
}

func GetUpdateLeaderArgsBytes(ula *UpdateLeaderArgs) [32]byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, (*ula).Id)
	binary.Write(buf, binary.LittleEndian, (*ula).LeaderID)
	return sha256.Sum256(buf.Bytes())
}
