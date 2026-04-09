export interface TournamentItem {
  id: string;
  name: string;
  status: string;
  date: string;
  location: string;
  participants: number;
}

export interface PlayerItem {
  id: string;
  name: string;
  title: string;
  location: string;
  rating: number;
  change: number;
}

export interface GameItem {
  id: string;
  red_id: string;
  red_name: string;
  black_id: string;
  black_name: string;
  result: string;
  tournament_id: string;
  tournament_name: string;
  opening_id: string;
  date: string;
  moves: number;
  move_list: string;
  opening: string;
  analyzed: boolean;
}

export interface OpeningItem {
  id: string;
  name: string;
  games: number;
  winRate: { red: number; draw: number; black: number };
}

export interface RankingItem {
  id: string;
  rank: number;
  player_id: string;
  player_name: string;
  rating: number;
  change: number;
  games: number;
}
