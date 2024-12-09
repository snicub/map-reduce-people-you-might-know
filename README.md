# People You Might Know - Apache Hadoop MapReduce Implementation

This project implements a **People You Might Know** recommendation system using **Apache Hadoop MapReduce**. It analyzes mutual friendships from a social network dataset and generates personalized friend recommendations for each user. The implementation consists of two stages: calculating mutual friends and generating recommendations based on the mutual friends count.

---

## Features
1. **Mutual Friends Calculation**:
   - Identifies pairs of users and their mutual friends.
   - Excludes direct friendships from recommendations.

2. **Friend Recommendation Generation**:
   - Sorts recommendations based on the number of mutual friends (descending).
   - Resolves ties using lexicographic order of user IDs.
   - Provides up to the top 10 recommendations for each user.

3. **Intermediate Data Handling**:
   - Uses an intermediate output directory to handle results from the first stage for further processing.

---

## Implementation Details

### 1. **Mapper: Mutual Friends Calculation**
- **Input**: Each line contains a user and their comma-separated list of friends.
- **Output**: 
  - Direct friendships as `key: (user1, user2)` with value `"#"`.
  - Potential mutual friend pairs as `key: (friend1, friend2)` with value `<user>`.

#### Example:
**Input Line**:  
`A    B,C,D`

**Mapper Output**:  
- `A,B    #`  
- `B,C    A`  
- `B,D    A`  

---

### 2. **Reducer: Filtering and Mutual Friend Counting**
- **Input**: Key-value pairs of user pairs and their associated values from the mapper.
- **Output**: Number of mutual friends for non-direct friend pairs.

#### Example:
**Reducer Input**:  
`B,C    A,X,Y`

**Reducer Output**:  
- `B    C:3`  
- `C    B:3`

---

### 3. **Final Reducer: Recommendation Generation**
- **Input**: Each user and a list of potential friend recommendations with their mutual friend counts.
- **Output**: Up to the top 10 recommended users for each user, sorted by:
  1. Number of mutual friends (descending).
  2. User ID (lexicographic order) in case of a tie.

#### Example:
**Input to Reducer**:  
`A    B:3,C:2`

**Final Output**:  
`A    B,C`

---

## Job Workflow

### **Stage 1: Mutual Friends Calculation**
- **Job Name**: `Mutual Friends Calculation`
- **Mapper**: `MutualFriendsMapper`
- **Reducer**: `MutualFriendsReducer`
- **Intermediate Output**: Written to a temporary directory.

### **Stage 2: Recommendation Generation**
- **Job Name**: `Recommendation Generation`
- **Mapper**: `IdentityMapper` (pass-through)
- **Reducer**: `RecommendationReducer`
- **Final Output**: Written to the specified output directory.
