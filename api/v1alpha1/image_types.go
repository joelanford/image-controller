/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ConditionTypeReady       = "Ready"
	ConditionTypeProgressing = "Progressing"
	ConditionTypeFailing     = "Failing"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ImageSpec defines the desired state of Image
type ImageSpec struct {
	// Reference is a remote image reference of the form <registry>/<image>:<tag> or <registry>/<image>@<digest>
	Reference string `json:"reference"`

	// RefreshInterval is the interval at which the image should be refreshed. If nil, the image will not be refreshed.
	RefreshInterval *metav1.Duration `json:"refreshInterval,omitempty"`
}

// ImageStatus defines the observed state of Image
type ImageStatus struct {
	// Conditions is a list of conditions the image is in
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Digest is the digest of the current active image
	Digest string `json:"digest,omitempty"`

	// ContentURL is the URL of the content of the image
	ContentURL string `json:"contentURL,omitempty"`

	// ExpirationTime is the time at which the image will expire and need to be refreshed
	ExpirationTime *metav1.Time `json:"expirationTime,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// Image is the Schema for the images API
type Image struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ImageSpec   `json:"spec,omitempty"`
	Status ImageStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// ImageList contains a list of Image
type ImageList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Image `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Image{}, &ImageList{})
}
